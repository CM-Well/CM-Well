/**
  * Copyright 2015 Thomson Reuters
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


package cmwell.dc.stream

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorSystem, Cancellable, OneForOneStrategy, Props, Status}
import akka.event.{BusLogging, Logging}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream._
import akka.stream.scaladsl.{Flow, Framing, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import cmwell.dc.{LazyLogging, Settings}
import cmwell.dc.Settings._
import cmwell.dc.stream.MessagesTypesAndExceptions._
import cmwell.dc.stream.akkautils.{ConcurrentFlow, DebugStage}
import cmwell.util.collections._
import k.grid.{GridReceives, JvmIdentity}

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.matching.Regex
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.{Failure, Success, Try}
import scala.language.reflectiveCalls

/**
  * Created by gilad on 1/4/16.
  */
object DataCenterSyncManager extends LazyLogging {

  case class SyncerMaterialization(cancelSyncing: KillSwitch, nextUnSyncedPosition: Future[String])

  //represents the sync status including the stream status. Also contains the materialized value to cancel the stream.
  sealed trait SyncerStatus

  //A specific syncer for specific ID is warming up (gets the position key to start with)
  case class SyncerWarmUp(dataCenterId: String, location: String) extends SyncerStatus

  //A specific syncer for specific ID is running. the result is contained in the materialized value. Also the cencel mechanism is in the materialized value.
  case class SyncerRunning(dataCenterId: String, location: String, control: SyncerMaterialization, isCancelled: Boolean) extends SyncerStatus

  //A specidif syncer is done. The position key that should be used for the next sync is kept here.
  case class SyncerDone(dataCenterId: String, location: String, nextUnSyncedPositionKey: String) extends SyncerStatus

  val maxTsvLineLength = {
    val slashHttpsDotPossiblePrefix = "/https.".length
    val maxUrlLength = 2083
    val dateLength = 25
    val uuidLength = 32
    val indexTimeLength = 13
    val tabs = 3
    slashHttpsDotPossiblePrefix + maxUrlLength + dateLength + uuidLength + indexTimeLength + indexTimeLength + 1
  }

  case class DataCenterToken(id: String, qp: String, withHistory: Boolean) {
    def formatWith(f: (String,String,String) => String) =
      f(id, if (qp.isEmpty) "" else s",[$qp]", if (withHistory) "&with-history" else "")
  }

  val dataCenterIdTokenParser = new JavaTokenParsers {
    val id: Parser[String] = "[^?]+".r
    val qp: Parser[String] = "qp=" ~> "[^&]+".r
    val wh: Parser[String] = "with-history"
    val startsWithQp: Parser[(String, Boolean)] = (qp ~ ("&" ~ wh).?) ^^ {
      case q ~ Some(_ ~ w) => q -> true
      case q ~ _ => q -> false
    }
    val optionalPart: Parser[(String, Boolean)] = ("?" ~> (startsWithQp | wh ^^ (_ => "" -> true))).? ^^ (_.getOrElse("" -> false))
    val tokenParser: Parser[DataCenterToken] = (id ~ optionalPart) ^^ {
      case i ~ ((q, w)) => DataCenterToken(i, q, w)
    }

    def parse(dataCenterId: String): Try[DataCenterToken] = {
      parseAll[DataCenterToken](tokenParser, dataCenterId) match {
        case Success(dcToken, _) => scala.util.Success(dcToken)
        case NoSuccess(err, _) => scala.util.Failure(new Exception(err))
      }
    }
  }

  def parseTSVAndCreateInfotonDataFromIt(tsv: ByteString) = {
    val tabAfterPath = tsv.indexOfSlice(tab)
    val tabAfterLast = tsv.indexOfSlice(tab, tabAfterPath + 1)
    val tabAfterUuid = tsv.indexOfSlice(tab, tabAfterLast + 1)

    val path = tsv.take(tabAfterPath).utf8String
    val uuid = tsv.slice(tabAfterLast + 1, tabAfterUuid)
    val idxt = tsv.drop(tabAfterUuid).utf8String.trim.toLong

    logger.trace(s"parseTSVAndCreateInfotonDataFromIt: [path='$path',uuid='${uuid.utf8String}',idxt='$idxt']")
    InfotonData(InfotonMeta(path, uuid, idxt), empty)
  }

  def props(dstServersVec: Vector[(String, Option[Int])], manualSyncList: Option[String]): Props = {
    Props(new DataCenterSyncManager(dstServersVec, manualSyncList))
  }
}

class DataCenterSyncManager(dstServersVec: Vector[(String, Option[Int])], manualSyncList: Option[String]) extends Actor with LazyLogging {

  import DataCenterSyncManager._

  //todo: should I use the same ActorSystem or create a new one?
  //context.system
  implicit val sys = ActorSystem("stream-dc")
  implicit val mat = ActorMaterializer(ActorMaterializerSettings(sys))

  var cancelDcInfotonChangeCheck: Cancellable = _
  var currentSyncs: SyncMap = Map.empty

  override def preStart: Unit = {
    cancelDcInfotonChangeCheck = sys.scheduler.schedule(10.seconds, 41.seconds, self, CheckDcInfotonList)
    require(dstServersVec.nonEmpty, "must have at least 1 host to communicate with")
    logger.info("Starting DataCenterSyncManager instance on this machine")
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case e: Throwable => {
      logger.error("caught exception as supervisor (all running syncs will be cancelled)", e)
      cancelAllRunningSyncs()
      cancelDcInfotonChangeCheck.cancel()
      mat.shutdown()
      val f = sys.terminate()
      Await.result(f, Duration.Inf)
      Stop
    }
  }

  //map: data center ID -> syncer status (warm up, done or running with the materialized value)
  type SyncMap = Map[String, SyncerStatus]

  override def receive: Receive = {
    import akka.pattern._
    GridReceives.monitoring(sender).orElse {
      //todo: add check to not allow same id several times
      //todo: add another validation checks (e.g. two locations for one id)
      case CheckDcInfotonList => retrieveMonitoredDcList.map(RetrievedDcInfoList.apply).pipeTo(self)
      //todo: maybe to save the number of consecutive errors normally print warning after x times change error
      case Status.Failure(e) => logger.warn("Received Status failure ", e)
      case RetrievedDcInfoList(dcInfoInfotons) => handleSyncsAccordingToRetrievedDcInfoList(currentSyncs, dcInfoInfotons)
      case StartDcSync(dcInfo) => {
        //All the information is available (including the position key to start with) - start the sync engine
        val runningSyncMaterialization = runSyncingEngine(dcInfo)
        // even if warmed up (and the key is already in the map) the new value will replace the old one
        val newSyncMap: SyncMap = currentSyncs + (dcInfo.id -> SyncerRunning(dcInfo.id, dcInfo.location, runningSyncMaterialization, isCancelled = false))
        currentSyncs = newSyncMap
      }
      case WarmUpDcSync(dcInfo) => {
        //New sync request is in progress (getting the last synced index time and/or the position key to use)
        //The reason to keep this information is as a flag to know that the sync is starting and not start getting the index time/position key again and again.
        val newSyncMap: SyncMap = currentSyncs + (dcInfo.id -> SyncerWarmUp(dcInfo.id, dcInfo.location))
        currentSyncs = newSyncMap
      }
      case SetDcSyncAsCancelled(dcInfo) => {
        val syncerRunning = currentSyncs(dcInfo.id).asInstanceOf[SyncerRunning]
        val newSyncMap: SyncMap = currentSyncs + (dcInfo.id -> syncerRunning.copy(isCancelled = true))
        currentSyncs = newSyncMap
      }
      case RemoveDcSync(dcInfo) => {
        //The sync is done with an error. Remove the ID totally from the map. Next time, the position key will be taken from the data itself.
        val newSyncMap: SyncMap = currentSyncs - dcInfo.id
        currentSyncs = newSyncMap
      }
      // The user removed the sync infoton (or changed its properties) - remove it (in case of a change on the next schedule, a new sync will be started)
      case StopDcSync(dcInfo) => handleStopDcSync(dcInfo, currentSyncs)
      case SaveDcSyncDoneInfo(dcInfo) => {
        //The sync was completed successfully. Save the position key for the next sync to start.
        val newSyncMap: SyncMap = currentSyncs - dcInfo.id + (dcInfo.id -> SyncerDone(dcInfo.id, dcInfo.location, dcInfo.positionKey.get))
        currentSyncs = newSyncMap
      }
    }
  }

  override def postStop(): Unit = {
    logger.warn("DcSyncManager died. Cancelling all the running syncs. They will actually stop after all already got infotons will be written")
    cancelDcInfotonChangeCheck.cancel()
    cancelAllRunningSyncs()
  }

  private def cancelAllRunningSyncs(): Unit = {
    currentSyncs.collect {
      case (_, SyncerRunning(_, _, control, isCancelled)) if !isCancelled => control
    }.foreach(_.cancelSyncing.shutdown())
  }

  /**
    * Signlas the sync stream to stop.
    *
    * @param dcToStop
    * @param oldSyncMap
    */
  def handleStopDcSync(dcToStop: DcInfo, oldSyncMap: SyncMap): Unit = {
    val runningSyncToStop = oldSyncMap.get(dcToStop.id).fold {
      logger.error(s"Got stop request for data center ID ${dcToStop.id} that doesn't exist in the sync map. Not reasonable ! Current IDs in map are: ${oldSyncMap.keys.mkString(",")}")
    } {
      case syncer@SyncerRunning(dcId, location, control, isCancelled) if location == dcToStop.location && !isCancelled => {
        logger.info(s"Cancelling sync engine for data center id: $dcId from location ${dcToStop.location}. The sync will actually stop after all already got infotons will be written.")
        self ! SetDcSyncAsCancelled(dcToStop)
        control.cancelSyncing.shutdown()
      }
      case SyncerRunning(dcId, location, control, _) if location == dcToStop.location => {
        logger.error(s"Got stop request for data center ID ${dcToStop.id} from location ${dcToStop.location}. The sync from this location is already cancelled. Not reasonable !")
      }
      case SyncerRunning(dcId, location, control, _) => {
        logger.error(s"Got stop request for data center ID ${dcToStop.id} from location ${dcToStop.location}. The running sync is from location $location. Not reasonable !")
      }
      case SyncerDone(dcId, location, nextUnSyncedPositionKey) => {
        logger.error(s"Got stop request for data center ID ${dcToStop.id} from location ${dcToStop.location}. The sync for this data center ID is already done. Not reasonable !")
      }
      case SyncerWarmUp(dcId, location) => {
        logger.info(s"Got stop request for data center ID ${dcToStop.id} from location ${dcToStop.location}. The sync for this data center ID is still warming up from previous start request. It will be stopped only on the next schedule check after it will be fully running.")
      }
    }
  }

  def handleSyncsAccordingToRetrievedDcInfoList(previousSyncs: SyncMap, newDcInfoList: Seq[DcInfo]): Unit = {
    //ignore Dcs that are in the previous map and doesn't have yet finished(=don't have return value yet even if already have been cancelled)
    //It checks only the id not the actual location the data was taken from, so it can continue sync of an id even from different location.
    val dcsToStart: Seq[DcInfo] = newDcInfoList.collect {
      //no previous run - use the dcInfo from the user
      case dcInfo if !previousSyncs.contains(dcInfo.id) => {
        logger.info(s"Got sync request for data center id: ${dcInfo.id} from location ${dcInfo.location}${dcInfo.tsvFile.fold("")(f => s" using local file $f")} from the user.")
        dcInfo
      }
      // previous run exists - use the position key from the last successful run
      case dcInfo if previousSyncs.exists(t => dcInfo.id == t._1 && t._2.isInstanceOf[SyncerDone]) => {
        val positionKey = previousSyncs(dcInfo.id).asInstanceOf[SyncerDone].nextUnSyncedPositionKey
        logger.info(s"Got sync request for data center id: ${dcInfo.id} from location ${dcInfo.location} from the user. Using position key $positionKey from previous sync")
        dcInfo.copy(positionKey = Some(positionKey))
      }
    }
    handleDcsToStart(dcsToStart)

    previousSyncs.foreach {
      case (_, SyncerRunning(dcId, location, control, isCancelled)) if !isCancelled => {
        //stop each running sync (the sync infoton is deleted) that wasn't stopped before. Only stop syncs that already started, not warming up (the warming up ones will be stopped the next schedule after they will be started)
        if (!newDcInfoList.exists((dcInfo: DcInfo) => dcId == dcInfo.id && location == dcInfo.location)) {
          logger.info(s"Got stop sync request for data center id: $dcId from location $location from the user.")
          self ! StopDcSync(DcInfo(dcId, location))
        }
        //restart syncs that the location in the new sync list is different from the current one. It's done by stopping the sync. On the next schedule the sync with the new location will be started.
        //todo: also restart if the sync path is changed
        if (newDcInfoList.exists((dcInfo: DcInfo) => dcId == dcInfo.id && location != dcInfo.location)) {
          logger.info(s"The user changed the sync request for data center id: $dcId from location $location to another. Stopping the current sync. On the next schedule the new sync will begin.")
          self ! StopDcSync(DcInfo(dcId, location))
        }
      }
      case _ =>
    }
  }

  private def handleDcsToStart(dcsToStart: Seq[DcInfo]): Unit = {
    dcsToStart.foreach {
      case dcInfo@DcInfo(dataCenterId, location, idxTimeFromUser, keyFromFinishedRun, tsvFile) => {
        // if next unsynced position is available from previous sync run take it
        keyFromFinishedRun match {
          // We already have the key, send the start sync message immediately
          case Some(positionKey) => {
            logger.info(s"Starting sync for data center id $dataCenterId from location $location using position key $positionKey got from previous successful sync")
            self ! StartDcSync(dcInfo.copy(positionKey = Some(positionKey)))
          }
          case None if tsvFile.nonEmpty => {
            logger.info(s"Starting sync for data center id $dataCenterId from location $location using local file ${tsvFile.get} got from the user")
            self ! StartDcSync(dcInfo)
          }
          case None => {
            //first flag this sync as a warming up (due to futures processing until actually starting up)
            logger.info(s"Warming up (getting position key and if needed also last synced index time) sync engine for data center id: $dataCenterId, position key to start from not found.")
            self ! WarmUpDcSync(dcInfo)
            //take the index time (from parameter and if not get the last one from the data itself, if this is the first time syncing take larger than epoch) and create position key from it
            val idxTime = idxTimeFromUser match {
              case Some(l) => Future.successful(Some(l))
              case None => retrieveLocalLastIndexTimeForDataCenterId(dataCenterId).flatMap { idxTime =>
                idxTime.fold(Future.successful(Option.empty[Long]))(time => retrieveIndexTimeInThePastToStartWith(dataCenterId, time, location))
              }
            }
            idxTime.flatMap(indexTimeToPositionKey(dataCenterId, location, _)).onComplete {
              case Failure(e) => {
                logger.warn("Getting index time or position key failed. Cancelling the sync start. It will be started again on the next schedule check", e)
                self ! RemoveDcSync(dcInfo)
              }
              case Success(positionKey) => {
                logger.info(s"Starting sync for data center id $dataCenterId from location $location using position key $positionKey")
                self ! StartDcSync(dcInfo.copy(positionKey = Some(positionKey)))
              }
            }
          }
        }
      }
    }
  }

  def parseProcDcJsonAndGetLastIndexTime(body: ByteString): Option[Long] = {
    import spray.json._

    try {
      (JsonParser(ParserInput(body.toArray))
        .asJsObject
        .fields("fields")
        .asJsObject
        .fields("lastIdxT"): @unchecked) match {
        case JsArray(data) => data.headOption.flatMap {
          case num: JsNumber if num.value.longValue() == 0 => None
          case num: JsNumber => Some(num.value.longValue())
          case _ => throw new RuntimeException("must be a JsNumber!")
        }
      }
    } catch {
      case e: Exception => {
        logger.warn("failed parsing /meta/sys/dc data", e)
        throw e
      }
    }
  }

  def retrieveMonitoredDcList = {
    logger.info("Checking the current status of the syncing requests infotons")
    if (manualSyncList.nonEmpty) {
      Future.successful(parseMetaDcJsonAndGetDcInfoSeq(ByteString(manualSyncList.get)))
    }
    else {
      val (h, p) = randomFrom(dstServersVec)
      val request = HttpRequest(uri = s"http://$h${p.fold("")(":" + _)}/meta/sys/dc?op=search&with-data&format=json") -> MetaSysSniffer
      val flow = Http().superPool[ReqType]().map {
        case (Success(HttpResponse(s, _, entity, _)), MetaSysSniffer) if s.isSuccess() => {
          entity
            .dataBytes
            .runFold(empty)(_ ++ _)
            .map(parseMetaDcJsonAndGetDcInfoSeq)
        }
        case (Success(HttpResponse(s, _, entity, _)), _) => {
          val e = new Exception(s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: ${Await.result(entity.dataBytes.runFold(empty)(_ ++ _), Duration.Inf).utf8String}")
          val ex = RetrieveSyncInfotonsException(s"Retrieve sync infoton list failed. Using local machine $h:$p", e)
          Future.failed[Seq[DcInfo]](ex)
        }
        case (Failure(e), _) => {
          val ex = RetrieveSyncInfotonsException(s"Retrieve sync infoton list failed. Using local machine $h:$p", e)
          Future.failed[Seq[DcInfo]](ex)
        }
      }
      Source.single(request).via(flow).toMat(Sink.head)(Keep.right).run().flatMap(identity)
    }
  }

  def parseMetaDcJsonAndGetDcInfoSeq(body: ByteString): Seq[DcInfo] = {
    import spray.json._
    try {
      JsonParser(ParserInput(body.toArray))
        .asJsObject
        .fields("results")
        .asJsObject
        .fields("infotons") match {
        case JsArray(data) => data.map { d =>
          val f = d.asJsObject.fields("fields")
          val location = f.asJsObject.fields("location") match {
            case JsArray(Vector(JsString(loc))) => loc
          }
          val dataCenterId = f.asJsObject.fields("id") match {
            case JsArray(Vector(JsString(id))) => id
          }
          val userQp = f.asJsObject.fields.get("qp").map {
            case JsArray(Vector(JsString(qp))) => qp
          }
          val userWh = f.asJsObject.fields.get("with-history").map {
            case JsArray(Vector(JsString(wh))) => wh
          }
          val fromIndexTime = f.asJsObject.fields.get("fromIndexTime").map {
            case JsArray(Vector(JsNumber(idxTime))) => idxTime.toLong
          }
          val tsvFile = f.asJsObject.fields.get("tsvFile").map {
            case JsArray(Vector(JsString(file))) => file
          }
          val qpStr = userQp.fold("")(qp => s"qp=$qp")
          val whStr = userWh.fold("with-history")(wh => if (wh == "true") "with-history" else "")
          val qpAndWhStr = (for (str <- List(qpStr, whStr) if str.nonEmpty) yield str).mkString("&")
          val qpAndWhStrFinal = if (qpAndWhStr.length == 0) "" else "?" + qpAndWhStr
          DcInfo(s"$dataCenterId$qpAndWhStrFinal", location, fromIndexTime, tsvFile = tsvFile)
        }
        case _ => Seq.empty
      }
    } catch {
      case e: Exception => {
        logger.warn("failed parsing /meta/sys/dc data", e)
        Seq.empty
      }
    }
  }

  def indexTimeToPositionKey(dataCenterId: String, remoteLocation: String, idxTime: Option[Long]): Future[String] = {
    logger.info(s"Getting position key ${idxTime.fold("without using index time")("using last index time " + _)} for data center id: $dataCenterId and location $remoteLocation")
    dataCenterIdTokenParser.parse(dataCenterId).map { dataCenterToken =>
      val requestUri = dataCenterToken.formatWith { (id, qp, wh) =>
        s"http://$remoteLocation/?op=create-consumer&qp=-system.parent.parent_hierarchy:/meta/,-system.parent.parent_hierarchy:/docs/,system.dc::$id$qp$wh&with-descendants${idxTime.fold("")("&index-time=" + _)}"
      }
      logger.info(s"The get position key request for data center ID $dataCenterId is: $requestUri")
      val req = HttpRequest(uri = requestUri)
      Source.single(req -> CreateConsumer).via(Http().superPool[ReqType]()).runWith(Sink.head).flatMap {
        case (Success(res), CreateConsumer) if res.status.isSuccess() && res.headers.exists(_.name == "X-CM-WELL-POSITION") => {
          // while both of the following issues are still open:
          // https://github.com/akka/akka/issues/18540
          // https://github.com/akka/akka/issues/18716
          // we must consume the empty entity body
          res.entity.dataBytes.runWith(Sink.ignore)
          Future(res.getHeader("X-CM-WELL-POSITION").get.value())
        }
        case (Success(HttpResponse(s, _, entity, _)), _) => {
          val e = new Exception(s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: ${Await.result(entity.dataBytes.runFold(empty)(_ ++ _), Duration.Inf).utf8String}")
          val ex = CreateConsumeException(s"Create consume failed. Data center ID $dataCenterId, using remote location $remoteLocation", e)
          Future.failed[String](ex)
        }
        case (Failure(e), _) => {
          val ex = CreateConsumeException(s"Create consume failed. Data center ID $dataCenterId, using remote location $remoteLocation", e)
          Future.failed[String](ex)
        }
      }
    }.recover {
      case err: Throwable => Future.failed(err)
    }
      .get
  }
  def retrieveLocalLastIndexTimeForDataCenterId(dataCenterId: String): Future[Option[Long]] = {
    logger.info(s"Getting last synced index time from data for data center id: $dataCenterId")
    val (h, p) = randomFrom(dstServersVec)
    val dst = p.fold(h)(h + ":" + _)
    //The below request supports qp also (the qp it the last part of the ID and will be sent to the local server)
    val requestUri = s"http://$dst/proc/dc/$dataCenterId${if (dataCenterId.contains("?")) "&" else "?"}format=json"
    logger.info(s"The get last index time request for data center ID $dataCenterId is: $requestUri")
    val request = HttpRequest(uri = requestUri) -> ProcDcViewer
    val flow = {
      Http().superPool[ReqType]().map {
        case (Success(res@HttpResponse(s, _, entity, _)), ProcDcViewer) if s.isSuccess() => {
          logger.debug(s"got response from /proc/dc: $res")
          entity
            .dataBytes
            .runFold(empty)(_ ++ _)
            .map(parseProcDcJsonAndGetLastIndexTime)
        }
        case (Success(HttpResponse(s, _, entity, _)), _) => {
          val e = new Exception(s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: ${Await.result(entity.dataBytes.runFold(empty)(_ ++ _), Duration.Inf).utf8String}")
          val ex = GetIndexTimeException(s"Get index time failed. Data center ID $dataCenterId, using local machine $h:$p", e)
          Future.failed[Option[Long]](ex)
        }
        case (Failure(e), _) => {
          val ex = GetIndexTimeException(s"Get index time failed. Data center ID $dataCenterId, using local machine $h:$p", e)
          Future.failed[Option[Long]](ex)
        }
      }
    }
    Source.single(request).via(flow).toMat(Sink.head)(Keep.right).run().flatMap(identity)
  }

  def retrieveIndexTimeInThePastToStartWith(dataCenterId: String, localIndexTime: Long, remoteLocation: String): Future[Option[Long]] = {
    val infotonsToGoBack = 5000
    logger.info(s"Getting index time starting with $infotonsToGoBack th infoton before index time $localIndexTime got from the local data. for data center id: $dataCenterId")
    val remoteTsvList = retrieveTsvListUntilIndexTime(dataCenterId, remoteLocation, localIndexTime, infotonsToGoBack)
    remoteTsvList.flatMap { remoteList =>
      if (remoteList.size < infotonsToGoBack) {
        logger.info(s"Got less than $infotonsToGoBack infotons from remote location $remoteLocation for data center id $dataCenterId. Starting the sync from the beginning")
        Future.successful(None)
      }
      else {
        val (h, p) = randomFrom(dstServersVec)
        val localDst = p.fold(h)(h + ":" + _)
        //the first value will contain the earliest index time (the last element got is the first in the list)
        val localTsvSet = retrieveTsvListFromIndexTime(dataCenterId, localDst, remoteList.head.meta.indexTime, infotonsToGoBack)
        localTsvSet.map { localSet =>
          val idxTimeToStartWith = remoteList.find(infotonData => !localSet.contains(infotonData)).fold(localIndexTime)(_.meta.indexTime)
          Some(idxTimeToStartWith - 1)
        }
      }
    }
  }

  def retrieveTsvListUntilIndexTime(dataCenterId: String, location: String, indexTime: Long, infotonsToGoBack: Int): Future[List[InfotonData]] = {
    logger.info(s"Getting list of $infotonsToGoBack infotons from location $location before index time $indexTime from the local data. for data center id: $dataCenterId")
    //    val (dcId, userQp) = extractIdAndQpFromDataCenterId(dataCenterId)
    dataCenterIdTokenParser.parse(dataCenterId).map { dataCenterToken =>
      val requestUri = dataCenterToken.formatWith { (id, qp, wh) =>
        s"http://$location/?op=search&recursive&sort-by=-system.indexTime&qp=system.indexTime<<$indexTime,-system.parent.parent_hierarchy:/meta/,-system.parent.parent_hierarchy:/docs/,system.dc::$id$qp$wh&length=$infotonsToGoBack&format=tsv"
      }
      logger.info(s"The get list request for data center ID $dataCenterId is: $requestUri")
      val request = HttpRequest(uri = requestUri) -> SearchIndexTime
      val flow = {
        Http().superPool[ReqType]().map {
          case (Success(HttpResponse(s, _, entity, _)), SearchIndexTime) if s.isSuccess() => {
            entity
              .dataBytes
              .via(Framing.delimiter(endln, maximumFrameLength = maxTsvLineLength * 2))
              .runFold(List[InfotonData]())((total, bs) => parseTSVAndCreateInfotonDataFromIt(bs) :: total)
          }
          case (Success(HttpResponse(s, _, entity, _)), _) => {
            val e = new Exception(s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: ${Await.result(entity.dataBytes.runFold(empty)(_ ++ _), 20.seconds).utf8String}")
            val ex = GetInfotonListException(s"Get list of infotons. Data center ID $dataCenterId, using machine $location", e)
            Future.failed[List[InfotonData]](ex)
          }
          case (Failure(e), _) => {
            val ex = GetInfotonListException(s"Get list of infotons. Data center ID $dataCenterId, using machine $location", e)
            Future.failed[List[InfotonData]](ex)
          }
        }
      }
      Source.single(request).via(flow).toMat(Sink.head)(Keep.right).run().flatMap(identity)
    }.recover {
      case err: Throwable => Future.failed(err)
    }
      .get
  }

  def retrieveTsvListFromIndexTime(dataCenterId: String, location: String, indexTime: Long, infotonsToGoBack: Int): Future[Set[InfotonData]] = {
    logger.info(s"Getting list of $infotonsToGoBack infotons from location $location after index time $indexTime. for data center id: $dataCenterId")
    dataCenterIdTokenParser.parse(dataCenterId).map { dataCenterToken =>
      val requestUri = dataCenterToken.formatWith { (id, qp, wh) =>
        s"http://$location/?op=stream&recursive&qp=system.indexTime>>$indexTime,-system.parent.parent_hierarchy:/meta/,-system.parent.parent_hierarchy:/docs/,system.dc::$id$qp$wh&format=tsv"
      }
      logger.info(s"The get list request for data center ID $dataCenterId is: $requestUri")
      val request = HttpRequest(uri = requestUri) -> SearchIndexTime
      val flow = {
        Http().superPool[ReqType]().map {
          case (Success(res@HttpResponse(s, _, entity, _)), SearchIndexTime) if s.isSuccess() => {
            entity
              .dataBytes
              .via(Framing.delimiter(endln, maximumFrameLength = maxTsvLineLength * 2))
              .runFold(Set.empty[InfotonData])((total, bs) => total + parseTSVAndCreateInfotonDataFromIt(bs))
          }
          case (Success(HttpResponse(s, _, entity, _)), _) => {
            val e = new Exception(s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: ${Await.result(entity.dataBytes.runFold(empty)(_ ++ _), 20.seconds).utf8String}")
            val ex = GetInfotonListException(s"Get list of infotons. Data center ID $dataCenterId, using machine $location", e)
            Future.failed[Set[InfotonData]](ex)
          }
          case (Failure(e), _) => {
            val ex = GetInfotonListException(s"Get list of infotons. Data center ID $dataCenterId, using machine $location", e)
            Future.failed[Set[InfotonData]](ex)
          }
        }
      }
      Source.single(request).via(flow).toMat(Sink.head)(Keep.right).run().flatMap(identity)
    }.recover {
      case err: Throwable => Future.failed(err)
    }
      .get
  }
  def runSyncingEngine(dcInfo: DcInfo): SyncerMaterialization = {
    logger.info(s"Starting sync engine for data center id ${dcInfo.id} from location ${dcInfo.location}${dcInfo.positionKey.fold("")(key => s" using position key $key")}${dcInfo.tsvFile.fold("")(f => " and file " + f)}")
    val syncerMaterialization@SyncerMaterialization(cancelSyncing, nextUnSyncedPositionFuture) = createSyncingEngine(dcInfo).run()
    nextUnSyncedPositionFuture.onComplete {
      case Success(nextPositionKeyToSync) => {
        logger.info(s"The sync engine for data center id: ${dcInfo.id} from location ${dcInfo.location} stopped with success. The position key for next sync is: $nextPositionKeyToSync.")
        self ! SaveDcSyncDoneInfo(DcInfo(dcInfo.id, dcInfo.location, None, Some(nextPositionKeyToSync)))
      }
      case Failure(e) => {
        logger.error(s"Data center ID ${dcInfo.id}: syncing from location ${dcInfo.location} failed with exception: ", e)
        self ! RemoveDcSync(dcInfo)
      }
    }
    syncerMaterialization
  }

  def createSyncingEngine(dcInfo: DcInfo): RunnableGraph[SyncerMaterialization] = {
    val localDecider: Supervision.Decider = { e: Throwable =>
      logger.error(s"The stream of data center id ${dcInfo.id} from location ${dcInfo.location} got an exception caught in local decider. It will be stopped. The exception is:", e)
      Supervision.Stop
    }
    val tsvSource = dcInfo.tsvFile.fold(TsvRetriever(dcInfo, localDecider).mapConcat(identity))(_ => TsvRetrieverFromFile(dcInfo))
    val syncingEngine: RunnableGraph[SyncerMaterialization] =
      tsvSource
        //        .buffer(Settings.tsvBufferSize, OverflowStrategy.backpressure)
        .async
        .via(RatePrinter(dcInfo, _ => 1, "elements", "infoton TSVs from TSV source", 500))
        .via(InfotonAggregator(Settings.maxRetrieveInfotonCount, Settings.maxRetrieveByteSize, Settings.maxTotalInfotonCountAggregatedForRetrieve))
        //        .async
        .via(RatePrinter(dcInfo, bucket => bucket.size, "elements", "infoton TSVs from InfotonAggregator", 500))
        .via(ConcurrentFlow(Settings.retrieveParallelism)(InfotonRetriever(dcInfo, localDecider)))
        .mapConcat(identity)
        .async
        .via(RatePrinter(dcInfo, _.data.size / 1000D, "KB", "KB infoton Data from InfotonRetriever", 5000))
        .via(ConcurrentFlow(Settings.ingestParallelism)(InfotonAllMachinesDistributerAndIngester(dcInfo.id, dstServersVec, localDecider)))
        .toMat(Sink.ignore) {
          case (left, right) =>
            SyncerMaterialization(left._1, right.flatMap { _ =>
              logger.info(s"The Future of the sink of the stream of data center id ${dcInfo.id} from location ${dcInfo.location} finished with success. Still waiting for the position key.")
              left._2
            }
              .map { posKeys =>
                logger.info(s"The Future of the TSV retriever of the stream of data center id ${dcInfo.id} from location ${dcInfo.location} finished with success. The position keys got are: $posKeys")
                posKeys.last.getOrElse(posKeys.head.get)
              })
        }
        .withAttributes(ActorAttributes.supervisionStrategy(localDecider))
    syncingEngine
  }
}
