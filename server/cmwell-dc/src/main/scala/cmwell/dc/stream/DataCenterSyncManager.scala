/**
  * © 2019 Refinitiv. All Rights Reserved.
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
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream._
import akka.stream.scaladsl.{Framing, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import cmwell.dc.stream.MessagesTypesAndExceptions._
import cmwell.dc.stream.akkautils.ConcurrentFlow
import cmwell.dc.stream.algo.{AlgoFlow, DDPCAlgorithmJsonParser}
import cmwell.dc.{LazyLogging, Settings}
import cmwell.driver.Dao
import cmwell.util.collections._
import cmwell.zstore.ZStore
import k.grid.GridReceives
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.{Failure, Success, Try}

/**
  * Created by gilad on 1/4/16.
  */
object DataCenterSyncManager extends LazyLogging {

  case class SyncerMaterialization(cancelSyncing: KillSwitch,
                                   nextUnSyncedPosition: Future[String])

  //represents the sync status including the stream status. Also contains the materialized value to cancel the stream.
  sealed trait SyncerStatus

  //A specific syncer for specific ID is warming up (gets the position key to start with)
  case object SyncerWarmUp
      extends SyncerStatus

  //A specific syncer for specific ID is running. the result is contained in the materialized value. Also the cancel mechanism is in the materialized value.
  case class SyncerRunning(control: SyncerMaterialization, isCancelled: Boolean)
      extends SyncerStatus

  //A specific syncer is done. The position key that should be used for the next sync is kept here.
  case class SyncerDone(nextUnSyncedPositionKey: String)
      extends SyncerStatus

  val maxTsvLineLength = {
    val slashHttpsDotPossiblePrefix = "/https.".length
    val maxUrlLength = 2083
    val dateLength = 25
    val uuidLength = 32
    val indexTimeLength = 13
    val tabs = 3
    slashHttpsDotPossiblePrefix + maxUrlLength + dateLength + uuidLength + indexTimeLength + indexTimeLength + 1
  }

  case class DataCenterToken(id: String, dcType:String, qp: String, withHistory: Boolean) {
    def formatWith(f: (String, String, String, String) => String) =
      f(
        id,
        dcType,
        if (qp.isEmpty) "" else s",[$qp]",
        if (withHistory) "&with-history" else ""
      )
  }

  val dataCenterIdTokenParser = new JavaTokenParsers {
    val id: Parser[String] = "[^&]+".r
    val dcType :Parser[String] = "&type=" ~> "[^?]+".r
    val qp: Parser[String] = "qp=" ~> "[^&]+".r
    val wh: Parser[String] = "with-history"
    val startsWithQp: Parser[(String, Boolean)] = (qp ~ ("&" ~ wh).?) ^^ {
      case q ~ Some(_ ~ w) => q -> true
      case q ~ _           => q -> false
    }
    val optionalPart
      : Parser[(String, Boolean)] = ("?" ~> (startsWithQp | wh ^^ (
      _ => "" -> true
    ))).? ^^ (_.getOrElse("" -> false))
    val tokenParser: Parser[DataCenterToken] = (id ~ dcType ~ optionalPart) ^^ {
      case i ~ t ~ ((q, w)) => DataCenterToken(i, t, q, w)
    }

    def parse(dataCenterId: String): Try[DataCenterToken] = {
      parseAll[DataCenterToken](tokenParser, dataCenterId) match {
        case Success(dcToken, _) => scala.util.Success(dcToken)
        case NoSuccess(err, _)   => scala.util.Failure(new Exception(err))
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

    logger.trace(
      s"parseTSVAndCreateInfotonDataFromIt: [path='$path',uuid='${uuid.utf8String}',idxt='$idxt']"
    )
    InfotonData(BaseInfotonData(path, empty), uuid, idxt)
  }

  def props(dstServersVec: Vector[(String, Option[Int])],
            manualSyncList: Option[String]): Props = {
    Props(new DataCenterSyncManager(dstServersVec, manualSyncList))
  }
}


class DataCenterSyncManager(dstServersVec: Vector[(String, Option[Int])],
                            manualSyncList: Option[String])
    extends Actor
    with LazyLogging {

  import DataCenterSyncManager._

  //todo: should I use the same ActorSystem or create a new one?
  //context.system
  implicit val sys = ActorSystem("stream-dc")
  implicit val mat = ActorMaterializer(ActorMaterializerSettings(sys))
  lazy val dao = Dao(Settings.irwServiceDaoClusterName, Settings.irwServiceDaoKeySpace2, Settings.irwServiceDaoHostName, 9042, initCommands = None)
  lazy val zStore : ZStore = ZStore(dao)


  var cancelDcInfotonChangeCheck: Cancellable = _
  var currentSyncs: SyncMap = Map.empty

  override def preStart: Unit = {
    cancelDcInfotonChangeCheck =
      sys.scheduler.schedule(10.seconds, 41.seconds, self, CheckDcInfotonList)
    require(
      dstServersVec.nonEmpty,
      "must have at least 1 host to communicate with"
    )
    logger.info("Starting DataCenterSyncManager instance on this machine")
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case e: Throwable => {
      logger.error(
        "caught exception as supervisor (all running syncs will be cancelled)",
        e
      )
      cancelAllRunningSyncs()
      cancelDcInfotonChangeCheck.cancel()
      mat.shutdown()
      val f = sys.terminate()
      Await.result(f, Duration.Inf)
      Stop
    }
  }

  //map: data center ID -> syncer status (warm up, done or running with the materialized value)
  type SyncMap = Map[DcInfoKey, SyncerStatus]

  override def receive: Receive = {
    import akka.pattern._
    GridReceives.monitoring(sender).orElse {
      //todo: add check to not allow same id several times
      //todo: add another validation checks (e.g. two locations for one id)
      case CheckDcInfotonList =>
        retrieveMonitoredDcList.map(RetrievedDcInfoList.apply).pipeTo(self)
      //todo: maybe to save the number of consecutive errors normally print warning after x times change error
      case Status.Failure(e) => logger.warn("Received Status failure ", e)
      case RetrievedDcInfoList(dcInfoInfotons) =>
        handleSyncsAccordingToRetrievedDcInfoList(currentSyncs, dcInfoInfotons)
      case StartDcSync(dcInfo) => {
        //All the information is available (including the position key to start with) - start the sync engine
        val runningSyncMaterialization = runSyncingEngine(dcInfo)
        // even if warmed up (and the key is already in the map) the new value will replace the old one
        val newSyncMap: SyncMap = currentSyncs + (dcInfo.key -> SyncerRunning(
          runningSyncMaterialization,
          isCancelled = false
        ))
        currentSyncs = newSyncMap
      }
      case WarmUpDcSync(dcInfo) => {
        //New sync request is in progress (getting the last synced index time and/or the position key to use)
        //The reason to keep this information is as a flag to know that the sync is starting and not start getting the index time/position key again and again.
        val newSyncMap: SyncMap = currentSyncs + (dcInfo.key -> SyncerWarmUp)
        currentSyncs = newSyncMap
      }
      case SetDcSyncAsCancelled(dcInfoKey) => {
        val syncerRunning = currentSyncs(dcInfoKey).asInstanceOf[SyncerRunning]
        val newSyncMap: SyncMap = currentSyncs + (dcInfoKey -> syncerRunning.copy(isCancelled = true))
        currentSyncs = newSyncMap
      }
      case RemoveDcSync(dcInfo) => {
        //The sync is done with an error. Remove the ID totally from the map. Next time, the position key will be taken from the data itself.
        val newSyncMap: SyncMap = currentSyncs - dcInfo.key
        currentSyncs = newSyncMap
      }
      // The user removed the sync infoton (or changed its properties) - remove it (in case of a change on the next schedule, a new sync will be started)
      case StopDcSync(dcInfo) => handleStopDcSync(dcInfo, currentSyncs)
      case SaveDcSyncDoneInfo(dcInfo) => {
        //The sync was completed successfully. Save the position key for the next sync to start.
        val newSyncMap: SyncMap = currentSyncs - dcInfo.key + (dcInfo.key -> SyncerDone(dcInfo.positionKey.get))
        currentSyncs = newSyncMap
        val dcType = Util.extractDcType(dcInfo.key.id)
        if(dcType != "remote")
          zStore.putString(dcType , dcInfo.positionKey.get).onComplete{
            case Success(res) =>
            case Failure(ex) => logger.error(s"Failed to persist ${dcType} position", ex)
          }

      }
    }
  }

  override def postStop(): Unit = {
    logger.warn(
      "DcSyncManager died. Cancelling all the running syncs. They will actually stop after all already got infotons will be written"
    )
    cancelDcInfotonChangeCheck.cancel()
    cancelAllRunningSyncs()
  }

  private def cancelAllRunningSyncs(): Unit = {
    currentSyncs
      .collect {
        case (_, SyncerRunning(control, isCancelled)) if !isCancelled => control
      }
      .foreach(_.cancelSyncing.shutdown())
  }

  /**
    * Signlas the sync stream to stop.
    *
    * @param dcKeyToStop
    * @param oldSyncMap
    */
  def handleStopDcSync(dcKeyToStop: DcInfoKey, oldSyncMap: SyncMap): Unit = {
    val runningSyncToStop = oldSyncMap
      .get(dcKeyToStop)
      .fold {
        logger.error(s"Got stop request for: $dcKeyToStop that doesn't exist in the sync map. " +
          s"Not reasonable ! Current IDs in map are: ${oldSyncMap.keys.mkString(",")}")
      } {
        case SyncerRunning(control, isCancelled) if !isCancelled =>
          logger.info(s"Cancelling sync engine for: $dcKeyToStop. " +
            s"The sync will actually stop after all already got infotons will be written.")
          self ! SetDcSyncAsCancelled(dcKeyToStop)
          control.cancelSyncing.shutdown()
        case SyncerRunning(_, _) =>
          logger.error(s"Got stop request for: $dcKeyToStop. " +
            s"The sync from this location is already cancelled. Not reasonable !")
        case SyncerDone(_) =>
          logger.error(s"Got stop request for: $dcKeyToStop. " +
            "The sync for this data center ID is already done. Not reasonable !")
        case SyncerWarmUp =>
          logger.info(s"Got stop request for: $dcKeyToStop. " +
            "The sync for this data center ID is still warming up from previous start request. " +
            "It will be stopped only on the next schedule check after it will be fully running.")
      }
  }

  def handleSyncsAccordingToRetrievedDcInfoList(
    previousSyncs: SyncMap,
    newDcInfoList: Seq[DcInfo]
  ): Unit = {
    //ignore Dcs that are in the previous map and doesn't have yet finished(=don't have return value yet even if already have been cancelled)
    //It checks only the id not the actual location the data was taken from, so it can continue sync of an id even from different location.
    val dcsToStart: Seq[DcInfo] = newDcInfoList.collect {
      //no previous run - use the dcInfo from the user
      case dcInfo if !previousSyncs.contains(dcInfo.key) =>
        logger.info(s"Got sync request for: ${dcInfo.key}${dcInfo.tsvFile.fold("")(f => s" using local file $f")} from the user.")
        dcInfo
      // previous run exists - use the position key from the last successful run
      case dcInfo if previousSyncs.exists(t => dcInfo.key == t._1 && t._2.isInstanceOf[SyncerDone]) =>
        val positionKey = previousSyncs(dcInfo.key).asInstanceOf[SyncerDone].nextUnSyncedPositionKey
        logger.info(s"Got sync request for: ${dcInfo.key} from the user. " +
          s"Using position key $positionKey from previous sync")
        dcInfo.copy(positionKey = Some(positionKey))
    }
    handleDcsToStart(dcsToStart)

    previousSyncs.foreach {
      case (dcInfoKey, SyncerRunning(_, isCancelled)) if !isCancelled =>
        // stop each running sync (the sync infoton is deleted) that wasn't stopped before. Only stop syncs that already
        // started, not warming up (the warming up ones will be stopped the next schedule after they will be started)
        if (!newDcInfoList.exists(dcInfo => dcInfoKey == dcInfo.key)) {
          logger.info(s"Got stop sync request for: $dcInfoKey from the user.")
          self ! StopDcSync(dcInfoKey)
        }
        // restart syncs that were changed in the new sync list (the id is the same but some other parameter isn't). It's done by stopping
        // the sync. On the next schedule the sync with the new location will be started.
        if (newDcInfoList.exists(dcInfo => dcInfoKey.id == dcInfo.key.id && dcInfoKey != dcInfo.key)) {
          logger.info(s"The user changed the sync request from: $dcInfoKey to another. " +
            "Stopping the current sync. On the next schedule the new sync will begin.")
          self ! StopDcSync(dcInfoKey)
        }
      case _ =>
    }
  }

  private def handleDcsToStart(dcsToStart: Seq[DcInfo]): Unit = {
    dcsToStart.foreach {
      case dcInfo@DcInfo(dcKey, dcInfoExtra, idxTimeFromUser, keyFromFinishedRun, tsvFile) => {
        // if next unsynced position is available from previous sync run take it
        keyFromFinishedRun match {
          // We already have the key, send the start sync message immediately
          case Some(positionKey) =>
              logger.info(s"Starting sync for: $dcKey using position key $positionKey got from previous successful sync.")
              self ! StartDcSync(dcInfo.copy(positionKey = Some(positionKey)))
          case None if tsvFile.nonEmpty =>
            logger.info(s"Starting sync for: $dcKey using local file ${tsvFile.get} got from the user")
            self ! StartDcSync(dcInfo)
          case None => {
            //first flag this sync as a warming up (due to futures processing until actually starting up)
            logger.info(s"Warming up (getting position key and if needed also last synced index time) sync engine for " +
              s"$dcKey, position key to start from not found.")
            self ! WarmUpDcSync(dcInfo)
            Util.extractDcType(dcKey.id) match{
              case "remote" =>
                // take the index time (from parameter and if not get the last one from the data itself,
                // if this is the first time syncing take larger than epoch) and create position key from it
                val idxTime = retrieveIndexTimeFromRemote(dcKey, idxTimeFromUser)
                startDcFromIndexTime(dcInfo, dcKey, idxTime)
              case _ =>
                retrievePositionFromZstoreAndStartDc(dcInfo, idxTimeFromUser)
            }
          }
        }
      }
    }
  }

  private def retrievePositionFromZstoreAndStartDc(dcInfo: DcInfo, idxTimeFromUser:Option[Long]) = {
    val position = zStore.getStringOpt(Util.extractDcType(dcInfo.key.id))
    position.onComplete {
      case Success(Some(zstorePosition)) =>
        logger.info(s"Got key $zstorePosition from zstore for dc info key ${dcInfo.key}")
        self ! StartDcSync(dcInfo.copy(positionKey = Some(zstorePosition)))
      case Success(None) => logger.info(s"Starting DDPC stream for key:${dcInfo.key.id}")
        startDcFromIndexTime(dcInfo, dcInfo.key, Future.successful(None))
      case Failure(e) =>
        logger.error(s"Warming up for dc key ${dcInfo.key} failed to retrieve position from zstore with exception: ", e)
        self ! RemoveDcSync(dcInfo)
    }
  }

  private def retrieveIndexTimeFromRemote(dcKey: DcInfoKey, idxTimeFromUser: Option[Long]) = {
    idxTimeFromUser match {
      case Some(l) => Future.successful(Some(l))
      case None =>
        retrieveLocalLastIndexTimeForDataCenterId(dcKey)
          .flatMap { idxTime =>
            idxTime.fold(Future.successful(Option.empty[Long]))(time => retrieveIndexTimeInThePastToStartWith(dcKey.id, time, dcKey.location))
          }
    }
  }

  private def startDcFromIndexTime(dcInfo:DcInfo, dcKey: DcInfoKey, idxTime: Future[Option[Long]]) = {
    idxTime
      .flatMap(indexTimeToPositionKey(dcKey.id, dcKey.location, _))
      .onComplete {
        case Failure(e) =>
          logger.warn(s"Sync $dcKey. Getting index time or position key failed. Cancelling the sync start. " +
            s"It will be started again on the next schedule check", e)
          self ! RemoveDcSync(dcInfo)
        case Success(positionKey) =>
          logger.info(s"Starting sync for: $dcKey using position key $positionKey")
          self ! StartDcSync(dcInfo.copy(positionKey = Some(positionKey)))
      }
  }

  def parseProcDcJsonAndGetLastIndexTime(body: ByteString): Option[Long] = {

    try {
      (Json.parse(body.toArray) \ "fields" \ "lastIdxT": @unchecked) match {
        case JsDefined(JsArray(data)) =>
          data.headOption.flatMap {
            case num: JsNumber if num.value.longValue == 0 => None
            case num: JsNumber                               => Some(num.value.longValue)
            case _                                           => throw new RuntimeException("must be a JsNumber!")
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
      Future.successful(
        parseMetaDcJsonAndGetDcInfoSeq(ByteString(manualSyncList.get))
      )
    } else {
      val (h, p) = randomFrom(dstServersVec)
      val request = HttpRequest(
        uri =
          s"http://$h${p.fold("")(":" + _)}/meta/sys/dc?op=search&with-data&format=json"
      ) -> MetaSysSniffer
      val flow = Http().superPool[ReqType]().map {
        case (Success(HttpResponse(s, _, entity, _)), MetaSysSniffer)
            if s.isSuccess() => {
          entity.dataBytes
            .runFold(empty)(_ ++ _)
            .map(parseMetaDcJsonAndGetDcInfoSeq)
        }
        case (Success(HttpResponse(s, _, entity, _)), _) => {
          val e = new Exception(
            s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: ${Await
              .result(entity.dataBytes.runFold(empty)(_ ++ _), Duration.Inf)
              .utf8String}"
          )
          val ex = RetrieveSyncInfotonsException(
            s"Retrieve sync infoton list failed. Using local machine $h:$p",
            e
          )
          Future.failed[Seq[DcInfo]](ex)
        }
        case (Failure(e), _) => {
          val ex = RetrieveSyncInfotonsException(
            s"Retrieve sync infoton list failed. Using local machine $h:$p",
            e
          )
          Future.failed[Seq[DcInfo]](ex)
        }
      }
      Source
        .single(request)
        .via(flow)
        .toMat(Sink.head)(Keep.right)
        .run()
        .flatMap(identity)
    }
  }


  def parseMetaDcJsonAndGetDcInfoSeq(body: ByteString): Seq[DcInfo] = {

    try {
      Json.parse(body.toArray) \ "results" \ "infotons" match {
        case JsDefined(JsArray(data)) =>
          data.map { d =>
            val f = d \ "fields"
            val location = f \ "location" match {
              case JsDefined(JsArray(seq))
                if seq.length == 1 && seq.head.isInstanceOf[JsString] =>
                seq.head.as[String]
            }
            val dataCenterId = f \ "id" match {
              case JsDefined(JsArray(seq))
                if seq.length == 1 && seq.head.isInstanceOf[JsString] =>
                seq.head.as[String]
            }
            val userQp = f \ "qp" match {
              case JsDefined(JsArray(seq))
                if seq.length == 1 && seq.head.isInstanceOf[JsString] =>
                Option(seq.head.as[String])
              case _ => None
            }
            val userWh = f \ "with-history" match {
              case JsDefined(JsArray(seq)) =>
                seq.headOption.collect {
                  case JsString(wh) => wh
                }
              case _ => None
            }
            val dcType = f \ "type" match {
              case JsDefined(JsArray(seq))
                if seq.length == 1 && seq.head.isInstanceOf[JsString] =>
                seq.head.as[String]
            }
            val fromIndexTime = f \ "fromIndexTime" match {
              case JsDefined(JsArray(seq))
                if seq.length == 1 && seq.head.isInstanceOf[JsNumber] =>
                Option(seq.head.as[Long])
              case _ => None
            }
            val tsvFile = f \ "tsvFile" match {
              case JsDefined(JsArray(seq))
                if seq.length == 1 && seq.head.isInstanceOf[JsString] =>
                Option(seq.head.as[String])
              case _ => None
            }
            val transformations = f \ "transformations" match {
              case JsDefined(JsArray(seq)) =>
                seq.collect {
                  case JsString(rule) => rule.split("->") match {
                    case Array(source, target) => (source, target)
                  }
                }.toMap
              case _ => Map.empty[String, String]
            }
            val qpStr = userQp.fold("")(qp => s"qp=$qp")
            val whStr = userWh.fold("with-history")(
              wh => if (wh == "true") "with-history" else ""
            )
            val qpAndWhStr = (for (str <- List(qpStr, whStr) if str.nonEmpty)
              yield str).mkString("&")
            val qpAndWhStrFinal =
              if (qpAndWhStr.length == 0) "" else "?" + qpAndWhStr
            val modifier = f \ "modifier" match {
              case JsDefined(JsArray(seq))
                if seq.length == 1 && seq.head.isInstanceOf[JsString] =>
                Some(seq.head.as[String])
              case _ => None
            }
            val (dcInfoExtra, ingestOp) = dcType match {
              case "remote" => (None, "_ow")
              case _ => (Some(DDPCAlgorithmJsonParser.extractAlgoInfo(f)), "_in")
            }
            val dcKey = DcInfoKey(s"$dataCenterId&type=$dcType$qpAndWhStrFinal", location, transformations, ingestOp, modifier)
            DcInfo(dcKey, dcInfoExtra, idxTime = fromIndexTime, tsvFile = tsvFile)
          }.toSeq
        case _ => Seq.empty
      }
    } catch {
      case e: Exception => {
        logger.warn("failed parsing /meta/sys/dc data", e)
        Seq.empty
      }
    }
  }

  def indexTimeToPositionKey(dataCenterId: String,
                             remoteLocation: String,
                             idxTime: Option[Long]): Future[String] = {
    val t = idxTime.fold("without using index time")("using last index time " + _)
    logger.info(s"Getting position key $t for data center id: $dataCenterId and location $remoteLocation")
    dataCenterIdTokenParser
      .parse(dataCenterId)
      .map { dataCenterToken =>
        val requestUri = dataCenterToken.formatWith { (id, _, qp, wh) =>
          val sb = new StringBuilder
          sb ++= "http://"
          sb ++= remoteLocation
          sb ++= "/?op=create-consumer&qp=-system.parent.parent_hierarchy:/meta/,-system.parent.parent_hierarchy:/docs/,system.dc::"
          sb ++= id
          sb ++= qp
          sb ++= wh
          sb ++= "&with-descendants"
          idxTime.foreach{ t =>
            sb ++= "&index-time="
            sb ++= t.toString
          }
          sb.result()
        }
        logger.info(s"The get position key request for data center ID $dataCenterId is: $requestUri")
        val req = HttpRequest(uri = requestUri)
        Source
          .single(req -> CreateConsumer)
          .via(Http().superPool[ReqType]())
          .runWith(Sink.head)
          .flatMap {
            case (Success(res), CreateConsumer)
                if res.status.isSuccess() && res.headers.exists(
                  _.name == "X-CM-WELL-POSITION"
                ) => {
              // while both of the following issues are still open:
              // https://github.com/akka/akka/issues/18540
              // https://github.com/akka/akka/issues/18716
              // we must consume the empty entity body
              res.entity.dataBytes.runWith(Sink.ignore)
              Future(res.getHeader("X-CM-WELL-POSITION").get.value())
            }
            case (Success(HttpResponse(s, _, entity, _)), _) => {
              val body = Await.result(entity.dataBytes.runFold(empty)(_ ++ _), Duration.Inf).utf8String
              val e = new Exception(s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: $body")
              val ex = CreateConsumeException(s"Create consume failed. Data center ID $dataCenterId, using remote location $remoteLocation", e)
              Future.failed[String](ex)
            }
            case (Failure(e), _) => {
              val ex = CreateConsumeException(s"Create consume failed. Data center ID $dataCenterId, using remote location $remoteLocation", e)
              Future.failed[String](ex)
            }
          }
      }
      .recover { case err: Throwable => Future.failed(err) }
      .get
  }
  def retrieveLocalLastIndexTimeForDataCenterId(dcKey: DcInfoKey): Future[Option[Long]] = {
    logger.info(s"Getting last synced index time from data for: $dcKey")
    val (h, p) = randomFrom(dstServersVec)
    val dst = p.fold(h)(h + ":" + _)
    //The below request supports qp also (the qp it the last part of the ID and will be sent to the local server)
    val transformedId = Util.transform(dcKey.transformations.toList, dcKey.id)
    val requestUri = s"http://$dst/proc/dc/$transformedId${if (transformedId.contains("?")) "&" else "?"}format=json"
    logger.info(s"Requesting last index time for: $dcKey. Request URI: $requestUri")
    val request = HttpRequest(uri = requestUri) -> ProcDcViewer
    val flow = {
      Http().superPool[ReqType]().map {
        case (Success(res @ HttpResponse(s, _, entity, _)), ProcDcViewer)
            if s.isSuccess() => {
          logger.debug(s"got response from /proc/dc: $res")
          entity.dataBytes
            .runFold(empty)(_ ++ _)
            .map(parseProcDcJsonAndGetLastIndexTime)
        }
        case (Success(HttpResponse(s, headers, entity, _)), _) => {
          val body = Await.result(entity.dataBytes.runFold(empty)(_ ++ _), Duration.Inf).utf8String
          val e = new Exception(s"Cm-Well returned bad response: status: ${s.intValue} headers: $headers reason: ${s.reason} body: $body")
          val ex = GetIndexTimeException(s"Get index time failed. Sync key: $dcKey, using local machine $h:$p", e)
          Future.failed[Option[Long]](ex)
        }
        case (Failure(e), _) => {
          val ex = GetIndexTimeException(s"Get index time failed. Sync key: $dcKey, using local machine $h:$p", e)
          Future.failed[Option[Long]](ex)
        }
      }
    }
    Source
      .single(request)
      .via(flow)
      .toMat(Sink.head)(Keep.right)
      .run()
      .flatMap(identity)
  }

  def retrieveIndexTimeInThePastToStartWith(
    dataCenterId: String,
    localIndexTime: Long,
    remoteLocation: String
  ): Future[Option[Long]] = {
    val infotonsToGoBack = 5000
    logger.info(s"Getting index time starting with $infotonsToGoBack th infoton before index time $localIndexTime got " +
                s"from the local data. for data center id: $dataCenterId")
    val remoteTsvList =
      retrieveTsvListUntilIndexTime(
        dataCenterId,
        remoteLocation,
        localIndexTime,
        infotonsToGoBack
      )
    remoteTsvList.flatMap { remoteList =>
      if (remoteList.size < infotonsToGoBack) {
        logger.info(s"Got less than $infotonsToGoBack infotons from remote location $remoteLocation for data center id " +
                    s"$dataCenterId. Starting the sync from the beginning")
        Future.successful(None)
      } else {
        val (h, p) = randomFrom(dstServersVec)
        val localDst = p.fold(h)(h + ":" + _)
        //the first value will contain the earliest index time (the last element got is the first in the list)
        val localTsvSet =
          retrieveTsvListFromIndexTime(
            dataCenterId,
            localDst,
            remoteList.head.indexTime,
            infotonsToGoBack
          )
        localTsvSet.map { localSet =>
          val idxTimeToStartWith = remoteList
            .find(infotonData => !localSet.contains(infotonData))
            .fold(localIndexTime)(_.indexTime)
          Some(idxTimeToStartWith - 1)
        }
      }
    }
  }

  def retrieveTsvListUntilIndexTime(
    dataCenterId: String,
    location: String,
    indexTime: Long,
    infotonsToGoBack: Int
  ): Future[List[InfotonData]] = {
    logger.info(s"Getting list of $infotonsToGoBack infotons from location $location before index time $indexTime from " +
                s"the local data. for data center id: $dataCenterId")
    //    val (dcId, userQp) = extractIdAndQpFromDataCenterId(dataCenterId)
    dataCenterIdTokenParser
      .parse(dataCenterId)
      .map { dataCenterToken =>
        val requestUri = dataCenterToken.formatWith { (id, _, qp, wh) =>
          val sb = new StringBuilder
          sb ++= "http://"
          sb ++= location
          sb ++= "/?op=search&recursive&sort-by=-system.indexTime&qp=system.indexTime<<"
          sb ++= indexTime.toString
          sb ++= ",-system.parent.parent_hierarchy:/meta/,-system.parent.parent_hierarchy:/docs/,system.dc::"
          sb ++= id
          sb ++= qp
          sb ++= wh
          sb ++= "&length="
          sb ++= infotonsToGoBack.toString
          sb ++= "&format=tsv"
          sb.result()
        }
        logger.info(
          s"The get list request for data center ID $dataCenterId is: $requestUri"
        )
        val request = HttpRequest(uri = requestUri) -> SearchIndexTime
        val flow = {
          Http().superPool[ReqType]().map {
            case (Success(HttpResponse(s, _, entity, _)), SearchIndexTime)
                if s.isSuccess() => {
              entity.dataBytes
                .via(
                  Framing
                    .delimiter(endln, maximumFrameLength = maxTsvLineLength * 2)
                )
                .runFold(List[InfotonData]())(
                  (total, bs) => parseTSVAndCreateInfotonDataFromIt(bs) :: total
                )
            }
            case (Success(HttpResponse(s, _, entity, _)), _) => {
              val e = new Exception(
                s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: ${Await
                  .result(entity.dataBytes.runFold(empty)(_ ++ _), 20.seconds)
                  .utf8String}"
              )
              val ex = GetInfotonListException(
                s"Get list of infotons. Data center ID $dataCenterId, using machine $location",
                e
              )
              Future.failed[List[InfotonData]](ex)
            }
            case (Failure(e), _) => {
              val ex = GetInfotonListException(
                s"Get list of infotons. Data center ID $dataCenterId, using machine $location",
                e
              )
              Future.failed[List[InfotonData]](ex)
            }
          }
        }
        Source
          .single(request)
          .via(flow)
          .toMat(Sink.head)(Keep.right)
          .run()
          .flatMap(identity)
      }
      .recover {
        case err: Throwable => Future.failed(err)
      }
      .get
  }

  def retrieveTsvListFromIndexTime(
    dataCenterId: String,
    location: String,
    indexTime: Long,
    infotonsToGoBack: Int
  ): Future[Set[InfotonData]] = {
    logger.info(s"Getting list of $infotonsToGoBack infotons from location $location after index time $indexTime. for data center id: $dataCenterId")
    dataCenterIdTokenParser
      .parse(dataCenterId)
      .map { dataCenterToken =>
        val requestUri = dataCenterToken.formatWith { (id, dcType, qp, wh) =>
          val sb = new StringBuilder
          sb ++= "http://"
          sb ++= location
          sb ++= "/?op=stream&recursive&qp=system.indexTime>>"
          sb ++= indexTime.toString
          sb ++= ",-system.parent.parent_hierarchy:/meta/,-system.parent.parent_hierarchy:/docs/,system.dc::"
          sb ++= id
          sb ++= qp
          sb ++= wh
          sb ++= "&format=tsv"
          sb.result()
        }
        logger.info(
          s"The get list request for data center ID $dataCenterId is: $requestUri"
        )
        val request = HttpRequest(uri = requestUri) -> SearchIndexTime
        val flow = {
          Http().superPool[ReqType]().map {
            case (Success(res @ HttpResponse(s, _, entity, _)), SearchIndexTime)
                if s.isSuccess() => {
              entity.dataBytes
                .via(
                  Framing
                    .delimiter(endln, maximumFrameLength = maxTsvLineLength * 2)
                )
                .runFold(Set.empty[InfotonData])(
                  (total, bs) => total + parseTSVAndCreateInfotonDataFromIt(bs)
                )
            }
            case (Success(HttpResponse(s, _, entity, _)), _) => {
              val e = new Exception(
                s"Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason} body: ${Await
                  .result(entity.dataBytes.runFold(empty)(_ ++ _), 20.seconds)
                  .utf8String}"
              )
              val ex = GetInfotonListException(
                s"Get list of infotons. Data center ID $dataCenterId, using machine $location",
                e
              )
              Future.failed[Set[InfotonData]](ex)
            }
            case (Failure(e), _) => {
              val ex = GetInfotonListException(
                s"Get list of infotons. Data center ID $dataCenterId, using machine $location",
                e
              )
              Future.failed[Set[InfotonData]](ex)
            }
          }
        }
        Source
          .single(request)
          .via(flow)
          .toMat(Sink.head)(Keep.right)
          .run()
          .flatMap(identity)
      }
      .recover {
        case err: Throwable => Future.failed(err)
      }
      .get
  }

  def runSyncingEngine(dcInfo: DcInfo): SyncerMaterialization = {
    val transformationsStr = if (dcInfo.key.transformations.nonEmpty) s" with transformations: ${dcInfo.key.transformations.mkString("[", ",", "]")}" else ""
    logger.info(
      s"Starting sync engine for: ${dcInfo.key}" +
        s"${dcInfo.positionKey.fold("")(key => s" using position key $key")}${dcInfo.tsvFile.fold("")(f => " and file " + f)}" +
        s"$transformationsStr"
    )
    val syncerMaterialization@SyncerMaterialization(_, nextUnSyncedPositionFuture) = createSyncingEngine(dcInfo).run()
    nextUnSyncedPositionFuture.onComplete {
      case Success(nextPositionKeyToSync) =>
        logger.info(s"The sync engine for: ${dcInfo.key} stopped with " +
          s"success. The position key for next sync is: $nextPositionKeyToSync.")
        self ! SaveDcSyncDoneInfo(DcInfo(dcInfo.key, dcInfo.dcAlgoData, positionKey = Some(nextPositionKeyToSync)))
      case Failure(e) =>
        logger.error(s"Sync ${dcInfo.key} failed with exception: ", e)
        self ! RemoveDcSync(dcInfo)
    }
    syncerMaterialization
  }

  def createSyncingEngine(
    dcInfo: DcInfo
  ): RunnableGraph[SyncerMaterialization] = {
    val localDecider: Supervision.Decider = { e: Throwable =>
      // The decider is not used anymore the restart is done by watching the Future[Done] of the stream - no need for
      // the log print (It's left for completeness until the decider is totally removed.
      logger.debug(s"The stream of sync ${dcInfo.key} got an exception caught" +
        s" in local decider. It inner stream will be stopped (the whole one may continue). The exception is:", e)
      Supervision.Stop
    }
    val tsvSourceWithBuffer = dcInfo.tsvFile.fold {
      val tsvSource = TsvRetriever(dcInfo, localDecider)
      val bufferedTsvSource = if (Settings.tsvBufferSize < 1) tsvSource else tsvSource.buffer(Settings.tsvBufferSize, OverflowStrategy.backpressure)
      bufferedTsvSource.mapConcat(identity)
    }(_ => TsvRetrieverFromFile(dcInfo))

    val infotonDataTransformer: BaseInfotonData => BaseInfotonData = Util.createInfotonDataTransformer(dcInfo)
    val syncingEngine: RunnableGraph[SyncerMaterialization] =
      tsvSourceWithBuffer
        .async
        .via(RatePrinter(dcInfo.key, _ => 1, "elements", "infoton TSVs from TSV source", 500))
        .via(InfotonAggregator[InfotonData](Settings.maxRetrieveInfotonCount, Settings.maxRetrieveByteSize,
          Settings.maxTotalInfotonCountAggregatedForRetrieve, _.base))
        //        .async
        .via(RatePrinter(dcInfo.key, bucket => bucket.size, "elements", "infoton TSVs from InfotonAggregator", 500))
        .via(ConcurrentFlow(Settings.retrieveParallelism)(InfotonRetriever(dcInfo.key, localDecider)))
        .mapConcat(identity)
        .via(AlgoFlow.algoFlow(dcInfo))
        .async
        .via(RatePrinter(dcInfo.key, _.data.size / 1000D, "KB", "KB infoton Data from InfotonRetriever", 5000))
        .map(infotonDataTransformer)
        .via(ConcurrentFlow(Settings.ingestParallelism)(InfotonAllMachinesDistributerAndIngester(dcInfo.key, dstServersVec,
          localDecider)))
        .toMat(Sink.ignore) {
          case (left, right) =>
            SyncerMaterialization(
              left._1,
              right
                .flatMap { _ =>
                  logger.info(s"The Future of the sink of the stream ${dcInfo.key} " +
                    s"finished with success. Still waiting for the position key.")
                  left._2
                }
                .map { posKeys =>
                  logger.info(s"The Future of the TSV retriever of the stream of sync ${dcInfo.key} " +
                    s"finished with success. The position keys got are: $posKeys")
                  posKeys.last.getOrElse(posKeys.head.get)
                }
            )
        }
        .withAttributes(ActorAttributes.supervisionStrategy(localDecider))
    syncingEngine
  }

}
