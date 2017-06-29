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


package cmwell.dc

import java.io.StringWriter

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.io.IO
import akka.util.Timeout
import cmwell.ctrl.config.Config._
import cmwell.ctrl.config.Jvms
import cmwell.ctrl.controllers.CmwellController
import cmwell.ctrl.hc._
import cmwell.ctrl.server._
import cmwell.ctrl.tasks.TaskExecutorActor
import cmwell.dc.stream.DataCenterSyncManager
import k.grid.service.ServiceTypes
import org.apache.jena.query.DatasetFactory
import k.grid.{GridConnection, GridReceives, Grid}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.graph.{NodeFactory, Node}
import org.slf4j.LoggerFactory
import spray.can.Http
import spray.http._
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import spray.client.pipelining._
import akka.actor._
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.scalalogging.{LazyLogging => LazyLoggingWithoutRed}
import cmwell.util.string._

trait LazyLogging extends LazyLoggingWithoutRed {
  protected lazy val redlog = LoggerFactory.getLogger("dc_red_log")
  protected lazy val yellowlog = LoggerFactory.getLogger("dc_yellow_log")
}
/*
 * Created by markz on 6/10/15.
 */
//////////////////////////////////////////
//                                      //
// NOT NEEDED, AND ANYWAY, WHY NOT USE: //
//                                      //
//     cmwell.util.string.Hash.md5      //
//                                      //
//////////////////////////////////////////
//
//import java.security.MessageDigest
//
//object md5 {
//  val digest = MessageDigest.getInstance("MD5")
//
//  def apply(data : String ) : String = {
//    digest.digest(data.getBytes).map("%02x".format(_)).mkString
//  }
//
//}

//case object Work
//case object Done
//case class PushChunk(chunk : List[String])
//case class Pushed(res: HttpResponse)
//case class FailedPush(req: HttpRequest,res: HttpResponse)

//class PushActor(pushDst : String, srcHostName : String) extends Actor with LazyLogging {
//  val io = IO(Http)(context.system)
//
//  override def receive = initialState()
//
//  class RetryPushActor(req: HttpRequest) extends Actor with LazyLogging {
//
//    def receive: Receive = retry(0)
//
//    def retry(count: Int): Receive = {
//      case r@HttpResponse(s,_,_,_) if s.isSuccess => context.parent ! Pushed(r)
//      case r@HttpResponse(s,_,_,_) if s.isFailure && count > Settings.maxRetries =>  context.parent ! FailedPush(req,r)
//      case r@HttpResponse(s,_,_,_) if s.isFailure => {
//        logger.warn(s"Failed to push $r")
//        sendTo(io).withResponsesReceivedBy(self)(req)
//        context.become(retry(count+1))
//      }
//    }
//  }
//
//  def chunkOfJsonLDQInfotonsToOneTrigString(chunk: List[String]): Try[String] = Try {
//    val ds = DatasetFactory.createMem()
//    val sw = new StringWriter()
//    chunk.foreach { line =>
//      //TMP LOGS
////      if (CopyDc.problematicUuids.exists(line.contains(_))) logger.warn(s"*** UUID ALERT (chunkOfJsonLDQInfotonsToOneTrigString) : $line ***")
//      //TMP LOGS
//      RDFDataMgr.read(ds, stringToInputStream(line), Lang.JSONLD)
//    }
//
//    val graph = ds.asDatasetGraph()
//
//    val fieldsToFilter = Set("uuid", "parent", "path", "length")
//    fieldsToFilter.foreach(field => graph.deleteAny(null, Node.ANY, NodeFactory.createURI(s"http://$srcHostName/meta/sys#$field"), Node.ANY))
//
//    RDFDataMgr.write(sw, graph, Lang.TRIG) //trig preserve prefixes, so it is better than nquads
//    sw.toString
//  }
//
//  def initialState(): Receive = {
//     case PushChunk(chunk) => {
//       logger.info(s"initialState: chunk.size = ${chunk.size}")
//       val c = chunk.take(Settings.pushChunkSize)
//       chunkOfJsonLDQInfotonsToOneTrigString(c) match {
//         case Success(dataToSend) => {
//           val rq = HttpRequest(HttpMethods.POST, uri = CopyDc.buildPushUrl(pushDst), entity = HttpEntity(dataToSend))
//           val ref = context.actorOf(Props(new RetryPushActor(rq)))
//           sendTo(io).withResponsesReceivedBy(ref)(rq)
//         }
//         case Failure(error) => {
//           logger.error("cannot create RDF from current chunk", error)
//           redlog.error("cannot create RDF from current chunk", error)
//           redlog.info(s"skipping first chunk: ${c.mkString("\n\t","\n\t","")}")
//           self ! PushChunk(chunk.drop(Settings.pushChunkSize))
//         }
//       }
//       context.become(waitForFirstReponse(chunk.drop(Settings.pushChunkSize)))
//     }
//  }
//
//  def waitForFirstReponse(restOfFirstChunk: List[String]): Receive = {
//    case Pushed(r@HttpResponse(s,e,h,p)) => {
//      sender ! PoisonPill
//      logger.debug(s"waitForFirstReponse: Pushed $r")
//
//      val hostToWorkWith = {
//        val fromHeader = h.find(_.name == "X-CMWELL-Hostname").map(_.value + ":9000")
//        logger.info(s"waitForFirstReponse: determine host from header X-CMWELL-Hostname: $fromHeader")
//        fromHeader.getOrElse(pushDst)
//      }
//
//      if (restOfFirstChunk.isEmpty) {
//        logger.info(s"waitForFirstReponse: first chunk was small, sending done.")
//        context.parent ! Done
//      } else {
//        self ! PushChunk(restOfFirstChunk)
//      }
//
//      context.become(workWith(hostToWorkWith, 0))
//    }
//    case FailedPush(req,r@HttpResponse(s,e,h,p)) => {
//      sender ! PoisonPill
//      logger.error(s"waitForFirstReponse: Failed to push $r")
//      redlog.error(s"waitForFirstReponse: Failed to push $r")
//      redlog.info(s"skipping first chunk, continue with what's left. skipped request: $req")
//      self ! PushChunk(restOfFirstChunk)
//      context.become(initialState())
//    }
//  }
//
//  def workWith(host: String, chunks: Int): Receive = {
//    case PushChunk(chunk) => {
//      logger.info(s"push chunk (chunks = $chunks, chunk.size = ${chunk.size})")
//      var chunksCount = 0
//      chunk.grouped(Settings.pushChunkSize).foreach { c =>
//       chunkOfJsonLDQInfotonsToOneTrigString(c) match {
//          case Success(dataToSend) => {
//            val rq = HttpRequest(HttpMethods.POST, uri = CopyDc.buildPushUrl(host), entity = HttpEntity(dataToSend))
//            val ref = context.actorOf(Props(new RetryPushActor(rq)))
//            sendTo(io).withResponsesReceivedBy(ref)(rq)
//            chunksCount += 1
//          }
//          case Failure(error) => {
//            logger.error("cannot create RDF from current chunk", error)
//            redlog.error("cannot create RDF from current chunk", error)
//            redlog.info(s"skipping current chunk: ${c.mkString("\n\t","\n\t","")}")
//            //TODO: retry? propogate failure up? send c's uuids to be substructed?
//          }
//        }
//      }
//      context.become(workWith(host, chunks+chunksCount))
//    }
//    //TODO: add retries in case of failure
//    case Pushed(r@HttpResponse(s,e,h,p)) => {
//      sender ! PoisonPill
//      logger.info(s"workWith: Pushed $r, chunks remain: $chunks")
//
//      if(chunks == 1) context.parent ! Done
//
//      context.become(workWith(host, chunks-1))
//    }
//    case FailedPush(req,r@HttpResponse(s,e,h,p)) => {
//      sender ! PoisonPill
//      logger.error(s"workWith: Failed to push $r")
//      redlog.error(s"workWith: Failed to push $r")
//      redlog.info(s"skipping request: $req")
//
//      if(chunks == 1) context.parent ! Done
//
//      context.become(workWith(host, chunks-1))
//    }
//  }
//}

//case object TimeoutSinceStart

//class DataStreamer(urlFunc: Function2[Long,Long,String], pushDst: String, fromTime: Long, toTime: Long, uuids: Set[(String,Long)], srcHostName : String, dstHostName: String, dataCenterId: String) extends Actor with LazyLogging {
//
//  val io = IO(Http)(context.system)
//  var newestIndexTime = 0L
//  var monitoringTime : Long = _
//  val stringResults = collection.mutable.ListBuffer[String]()
//  val uuidsBuffer = collection.mutable.ListBuffer[(String,Long)]()
//  val chunks = new scala.collection.mutable.Queue[List[String]]
//  val pushActor = context.actorOf(Props(new PushActor(pushDst, srcHostName)))
//  var forkedPullersToWaitFor = 0
//
//  override val supervisorStrategy = OneForOneStrategy() {
//    case _: ActorKilledException => Stop
//    case e: Throwable => {
//      logger.error(s"DataStreamer child died (${sender()})",e)
//      Stop
//    }
//  }
//
//  override def postRestart(t: Throwable): Unit = {
//    logger.info("after restart, sending `Work` to self.")
//    self ! Work
//  }
//
//  //5 minutes is a long time, which is needed since `Restart` an actor preserves the mail box.
//  //we want to ensure nothing is being processed and no messages are lest in the mailbox.
//  //this is a temporary hack. future version of the dc agent should handle this differently.
//  //(by, e.g: have the stream managed by another child actor)
//  val cancelable = context.system.scheduler.scheduleOnce(5.minutes,self,TimeoutSinceStart)
//  val timestamp = System.currentTimeMillis()
//
//  def getSysAttribute(attr: String, jString: String): Try[String] = {
//    val jsonldq = jString.parseJson.asJsObject
//
//    //if the infoton contain quads, get the default graph object
//    val json = Try[JsObject]{
//      val graphs = jsonldq.fields("@graph") match {
//        case JsArray(all) => all.map(_.asInstanceOf[JsObject])
//        case somethingElse => {
//          redlog.warn(s"""unable to process `jsonldq.fields("@graph")` on: $jsonldq\n""")
//          Vector.empty[JsObject]
//        }
//      }
//      graphs.find(_.fields.get("@graph").isEmpty).get
//    } match {
//      case Success(jsObj) => jsObj
//      case Failure(e) => jsonldq //if failed, return the original json
//    }
//
//    Try(json.fields(s"sys:$attr").asJsObject.fields.get("@value").collect{case JsString(x) => x}.get)
//      .recoverWith{
//      case _ : Throwable => Try(json.fields.get(attr).collect{case JsString(x) => x}.get)
//    } match {
//      case t@Success(v) => t
//      case t@Failure(err) => attr match {
//        case "indexTime" => {
//          logger.warn(s"could not retreive indexTime: getSysAttribute($attr,$jString)\n will attempt to convert lastModified into a fake index time.")
//          getSysAttribute("lastModified", jString).map{ date =>
//            cmwell.util.string.parseDate(date).get.getMillis.toString
//          }
//        }
//        case _ => {
//          logger.error(s"error in: getSysAttribute($attr,$jString)", err)
//          t
//        }
//      }
//    }
//  }
//
//  def partitionRangeForParallelism(f: Long, t: Long): List[(Long,Long)] = {
//    Try {
//      logger.info(s"splitting range ($f,$t) into ${Settings.parallelismLevel} parts")
//      val step = (t - f) / Settings.parallelismLevel + 1
//      val range = f to t by step
//      val xs = range.sliding(2).map(xs => xs.head -> (xs.last + 1)).toList
//      if(xs.last._2 < t) ((xs.last._2 - 1) -> t) :: xs
//      else xs
//    } match {
//      case Success(v) => {
//        logger.info(s"parallelizing pull with: $v")
//        v
//      }
//      case Failure(e) => {
//        logger.error("range arith exception?",e)
//        throw e
//      }
//    }
//  }
//
//  def receive: Receive = {
//    case Work => {
//      val pairs = partitionRangeForParallelism(fromTime,toTime)
//      pairs.foreach {
//        case (f, t) => {
//          val uri = urlFunc(f, t)
//          val rq = HttpRequest(HttpMethods.GET, uri = uri)
//          sendTo(io).withResponsesReceivedBy(self)(rq)
//          logger.info(s"DataStreamer: Connect to url $uri.")
//          monitoringTime = System.currentTimeMillis()
//        }
//      }
//      forkedPullersToWaitFor = pairs.length
//    }
//    case MessageChunk(entity,_) => {
//      val t2 = System.currentTimeMillis()
//      val delta = t2 - monitoringTime
//      logger.debug(s"DataStreamer: MessageChunk (time passed since start in millis: $delta)")
//      val data = entity.asString(HttpCharsets.`UTF-8`)
//      stringResults ++= data.lines
//    }
//    case ChunkedMessageEnd if forkedPullersToWaitFor > 1 => {
//      forkedPullersToWaitFor -= 1
//      logger.info(s"forkedPullersToWaitFor: 1 down, $forkedPullersToWaitFor remain")
//    }
//    case ChunkedMessageEnd => {
//      logger.info(s"DataStreamer: ChunkedMessageEnd (time passed since start in millis: ${System.currentTimeMillis() - monitoringTime})")
//      //all is good. we managed to pull all data! no need to send ourself `TimeoutSinceStart` message.
//      cancelable.cancel()
//      val sorted = stringResults.sortBy { str =>
//        getSysAttribute("indexTime", str).map(_.toLong) match {
//          case Success(lng) => lng
//          case Failure(err) => {
//            logger.error(s"DataStreamer: Chunked message sort failed: $str", err)
//            0L
//          }
//        }
//      }
//
//      //to guard from mem leaks & reuse
//      stringResults.clear()
//
//      val jsonLastLineChunk: List[String] = ((List.empty[String] -> Set.empty[String]) /: sorted) {
//        case ((jLines, paths), ldq) => {
//          //TODO: `.get` is dangerous. refactor naive code to be more resilient.
//          val path = getSysAttribute("path", ldq) match {
//            case Success(p) => p
//            case Failure(e) => {
//              val alt = "p-" + Hash.md5(ldq)
//              redlog.error(s"failed retrieving path (using alt = $alt) from $ldq")
//              alt
//            }
//          }
//          val uuid = getSysAttribute("uuid", ldq) match {
//            case Success(p) => p
//            case Failure(e) => {
//              val alt = "u-" + Hash.md5(ldq)
//              redlog.error(s"failed retrieving uuid (using alt = $alt) from $ldq")
//              alt
//            }
//          }
//          val idxT = getSysAttribute("indexTime", ldq).map(_.toLong) match {
//            case Success(p) => p
//            case Failure(e) => {
//              val alt = fromTime
//              redlog.error(s"failed retrieving indexTime (using alt = $alt) from $ldq")
//              alt
//            }
//          }
//
//          //TMP LOGS
////          if(CopyDc.problematicUuids.contains(uuid)) logger.warn(s"*** UUID ALERT (DataStreamer fold) : $path | $uuid | $idxT | collision = ${uuids.exists(_._1 == uuid)} ***")
//          //TMP LOGS
//
//          newestIndexTime = math.max(newestIndexTime,idxT)
//
//          if (uuids.exists(_._1 == uuid)) {
//            logger.info(s"uuid collision [$uuid] for path [$path]")
//            jLines -> paths
//          }
//          else {
//            uuidsBuffer += uuid -> idxT
//            if (paths(path)) {
//              if(jLines.nonEmpty) {
//                chunks.enqueue(jLines)
//              }
//              else {
//                logger.warn(s"IMPOSIBRU! ($path)")
//              }
//              List(ldq) -> Set(path)
//            }
//            else (ldq :: jLines) -> (paths + path)
//          }
//        }
//      }._1
//
//
//      if(jsonLastLineChunk.nonEmpty) {
//        chunks.enqueue(jsonLastLineChunk)
//        pushActor ! PushChunk(chunks.dequeue())
//        logger.info(s"last chunk pushed to pushActor. current #chunks = ${chunks.size}, uuidsBuffer.size = ${uuidsBuffer.size}")
//      }
//
//      //we got nothing new...
//      if(uuidsBuffer.isEmpty) {
//        logger.info("uuidsBuffer is empty: nothing new...")
//        //if we got here, it means we asked once for the data with overlap, but it returned the same data again.
//        //so we need to send the real last indexTime we have locally without overlap, so next pull will start without dups.
//        if(newestIndexTime == 0)
//          context.actorOf(Props(new LastIndexTimeRetriver(dstHostName, dataCenterId))) ! Retrieve
//        else
//          context.system.scheduler.scheduleOnce(3.seconds,context.parent,DoneSync(fromTime, toTime, newestIndexTime, Set.empty[(String,Long)]))
//      }
//    }
//    case LocalStatusForDC(_, lastIndexTime) => {
//      sender ! PoisonPill
//      newestIndexTime = lastIndexTime
//      context.system.scheduler.scheduleOnce(3.seconds,context.parent,DoneSync(fromTime, toTime, newestIndexTime, Set.empty[(String,Long)]))
//    }
//    case LocalErrorStatusForDC(_, error) => {
//      sender ! PoisonPill
//      logger.warn(s"Error in retrieving DC last indexTime: $error")
//      context.parent ! SyncFailure
//    }
//    case Done => {
//      logger.info(s"PushActor sent Done. remaining chunks: ${chunks.size}")
//      if (chunks.isEmpty) {
//        sender ! PoisonPill
//        logger.info(s"All data was sent to local dc.")
//        context.system.scheduler.scheduleOnce(3.seconds,context.parent,DoneSync(fromTime, toTime, newestIndexTime, uuidsBuffer.toSet))
//      }
//      else {
//        pushActor ! PushChunk(chunks.dequeue())
//      }
//    }
//    case TimeoutSinceStart => {
//      logger.error("TimeoutSinceStart: suicide!")
//      self ! Kill
//    }
//    case Failure(e: spray.can.Http.ConnectionException) => {
//      logger.error(s"got Failure(e: spray.can.Http.ConnectionException)",e)
//      if(forkedPullersToWaitFor < 2) {
//        logger.error("will suicide now.")
//        self ! Kill
//      }
//      else {
//        logger.warn(s"we don't want to pollute the restarted actor mailbox, so we wait for scheduler to kill us when 5 minutes pass in ${(System.currentTimeMillis() - timestamp) / 1000} seconds")
//      }
//    }
//    case unknown => logger.warn(s"unknown message received: $unknown")
//  }
//}

case class SyncData(fromTime: Long)
//case class DoSyncData(fromTime: Long, toTime: Long)
//case class DoneSync(fromTime: Long , toTime: Long, newestIndexTime: Long, newUuidsSetToFilter: Set[(String,Long)])
//case object SyncFailure
//case object RetrieveLastIndexTime

class DcSync( srcHostName : String , destinationHosts : Vector[String] ,dataCenterId : String) extends Actor with LazyLogging {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  val alternateMethod = Settings.altSyncMethod
  lazy val altSyncronizer = context.actorOf(Props(classOf[AltSyncronizer], srcHostName, destinationHosts, dataCenterId))

  var t = 0L

  def altSyncRecovery: Unit = {
    import akka.pattern.{pipe,ask}

    implicit val timeout = akka.util.Timeout(30.seconds)

    val f = (context.actorOf(Props(new RangeSearcher(srcHostName))) ? RefineSyncRange(dataCenterId, 0L, System.currentTimeMillis())).mapTo[RangeReponse]

    f.map{
      case NothingToSync => StartSyncFrom(0L)
      case RefinedSyncRange(_,fromIdx,_,_) => StartSyncFrom(fromIdx - Settings.overlap)
    } pipeTo altSyncronizer

    f.onFailure{ case err =>
      logger.error("altSyncRecovery failed !!!",err)
    }
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case _: ActorKilledException => {
      if(sender() == altSyncronizer) {
        altSyncRecovery
      }
      Restart
    }
    case e: Throwable => {
      logger.error(s"DcSync child died (${sender()})",e)

      if(sender() == altSyncronizer) {
        altSyncRecovery
        Restart
      }
      else {
        Stop
      }
    }
  }

  def receive : Receive = {
    case SyncData(fromTime) => {//if alternateMethod => {
      altSyncronizer ! StartSyncFrom(fromTime)
    }
//    case msg@SyncData(fromTime, toTime) => {
//      //TODO: populate a set of the last uuids we already have locally. i.e:
//      //1. Retrieve lastIndexTime (what to do if it's newer than `toTime`???)
//      //2. Get all infotons with indexTime > (lastIndexTime - overlap)
//      //3. Store uuids of infotons in a set, and continue with rest of the logic
//      context.become(receiveWithUuidCache(Set.empty[(String,Long)]))
//      self ! msg
//    }
  }

//  def receiveWithUuidCache(uuids: Set[(String,Long)]): Receive = {
//    case SyncData(fromTime, toTime) => {
//      logger.info(s"Working on $fromTime - $toTime")
//      t = toTime
//      context.actorOf(Props(new RangeSearcher(srcHostName))) ! RefineSyncRange(dataCenterId, fromTime, toTime)
//    }
//    case RefinedSyncRange(dc, fromT, toT, uuidsInRange) => {
//      //TMP LOGS
////      if(CopyDc.problematicUuids.exists(uuidsInRange.apply)) logger.warn(s"*** UUID ALERT (uuidsInRange) : ${CopyDc.problematicUuids.filter(uuidsInRange.apply).mkString("[",",","]")} ***")
//      //TMP LOGS
//      sender ! PoisonPill
//      if (uuidsInRange.forall(u=>uuids.exists(_._1 == u))) {
//        logger.info(s"RefinedSyncRange contains nothing new. uuids.size = ${uuidsInRange.size}")
//        if(uuidsInRange.size == Settings.maxInfotonsPerSync) {
//          logger.debug("uuidsInRange.size == Settings.maxInfotonsPerSync")
//          t = System.currentTimeMillis()
//          context.actorOf(Props(new RangeSearcher(srcHostName))) ! RefineSyncRange(dataCenterId, toT - 1, t)
//        }
//        else if(toT == t){
//          logger.debug("toT == t")
//          val newTo = System.currentTimeMillis() + 10000L
//          context.system.scheduler.scheduleOnce(10.seconds, self, SyncData(fromT, newTo))
//        }
//        else {
//          context.system.scheduler.scheduleOnce(10.seconds, self, RetrieveLastIndexTime)
//        }
//      }
//      else {
//        logger.info(s"DcSync: got RefinedSyncRange($dc, $fromT, $toT, uuidsInRange.size = ${uuidsInRange.size}, uuids.size = ${uuids.size})")
//        if(uuidsInRange.size <= 10) {
//          logger.info(s"small set of uuidsInRange: $uuidsInRange")
//        }
//        if(uuids.size <= 10) {
//          logger.info(s"small set of uuids: $uuids")
//        }
//        val urlFunc: Function2[Long,Long,String] = CopyDc.buildStreamUrl(srcHostName, dataCenterId, _, _)
//        val dataStreamer = context.actorOf(Props(new DataStreamer(urlFunc, dstHostName, fromT, toT, uuids, srcHostName, dstHostName: String, dataCenterId: String)))
//        dataStreamer ! Work
//      }
//    }
//    case TimeoutFromHttp(oldFrom) => {
//      sender ! PoisonPill
//      logger.warn("got TimeoutFromHttp")
//      t = System.currentTimeMillis()
//      self ! SyncData(oldFrom,t)
//    }
//    case NothingToSync => {
//      sender ! PoisonPill
//      logger.info("got NothingToSync")
//      context.system.scheduler.scheduleOnce(10.seconds, self, RetrieveLastIndexTime)
//    }
//    case DoneSync(fromTime, toTime, newestIndexTime, newSetOfUuids) => {
//      //TMP LOGS
////      if(CopyDc.problematicUuids.exists(u=>newSetOfUuids.exists(_._1==u))) logger.warn(s"*** UUID ALERT (newSetOfUuids) : ${newSetOfUuids.filter(t=>CopyDc.problematicUuids.contains(t._1)).mkString("[",",","]")} ***")
////      if(CopyDc.problematicUuids.exists(u=>uuids.exists(_._1==u))) logger.warn(s"*** UUID ALERT (uuids) : ${uuids.filter(t=>CopyDc.problematicUuids.contains(t._1)).mkString("[",",","]")} ***")
//      //TMP LOGS
//      val nowTime = System.currentTimeMillis()
//      logger.info(s"data was synced $fromTime - $toTime calc new sync date now $nowTime, newestIndexTime got $newestIndexTime, and t = $t, newSetOfUuids.size = ${newSetOfUuids.size}")
//      sender ! PoisonPill
//      if (t > toTime) {
//        val msg = SyncData(newestIndexTime - 1, nowTime)
//        logger.info(s"fire $msg, t = $t, toTime = $toTime (t > toTime)")
//        self ! msg
//      } else {
//        val delta = nowTime - newestIndexTime
//        if(delta < 10000L) {
//          logger.info(s"small delta = $delta")
//          context.system.scheduler.scheduleOnce((10000 - delta).millis, self, RetrieveLastIndexTime)
//        }
//        else {
//          logger.info(s"delta over 10 sec = $delta")
//          self ! SyncData(newestIndexTime,nowTime)
//        }
//      }
//      val uuidIndexTimeTuplesSet = uuids.filter(t=>newestIndexTime-t._2<=Settings.overlap) ++ newSetOfUuids
//
//      context.become(receiveWithUuidCache(uuidIndexTimeTuplesSet))
//    }
//    case SyncFailure => {
//      sender ! PoisonPill
//      self ! RetrieveLastIndexTime
//    }
//    case RetrieveLastIndexTime => {
//      logger.info("got RetrieveLastIndexTime")
//      context.actorOf(Props(new LastIndexTimeRetriver(dstHostName, dataCenterId))) ! Retrieve
//    }
//    case LocalStatusForDC(_, lastIndexTime) => {
//      sender ! PoisonPill
//      val f = math.max(0,lastIndexTime - Settings.overlap)
//      self ! SyncData(f, System.currentTimeMillis())
//    }
//    case LocalErrorStatusForDC(_, error) => {
//      sender ! PoisonPill
//      logger.warn(s"Error in retrieving DC last modified: $error")
//      context.system.scheduler.scheduleOnce(5000.millis, self, RetrieveLastIndexTime)
//    }
//  }
}
sealed trait RangeReponse
case class LocalLastUuids(us: Set[String], lastIdxTime: Long)
case class RefineSyncRange(dc: String, fromIdxT: Long, toIdxT: Long)
case class RefinedSyncRange(dc: String, fromIdxT: Long, toIdxT: Long, uuidsInRange: Set[String]) extends RangeReponse
case object NothingToSync extends RangeReponse
case class TimeoutFromHttp(oldFrom: Long)

class RangeSearcher(remote : String) extends Actor with LazyLogging {
  val minChunkSize = Settings.minInfotonsPerSync
  val maxChunkSize = Settings.maxInfotonsPerSync

  val io = IO(Http)(context.system)
  var cancellable: Cancellable = _

  def searchUrl(dc : String, from : Long, to : Long) = s"http://$remote/?op=search&qp=-system.parent.parent_hierarchy:/meta/,system.dc::$dc,system.indexTime>$from,system.indexTime<$to,system.lastModified>1970&with-descendants&format=tsv&with-history&sort-by=*system.indexTime&length=$maxChunkSize"

  private[this] var dc : String = _
  private[this] var from : Long = _
  private[this] var to : Long = _

  def extractIndexTimeAndUuid(line : String): (Long,String) = {
    val arr = line.split("\t")
    arr(3).toLong -> arr(2)
  }

  override def receive: Actor.Receive = {
    case msg@RefineSyncRange(dc, fromIdxT, toIdxT) => {
      this.dc = dc
      this.from = fromIdxT
      this.to = toIdxT

      val url = searchUrl(dc,fromIdxT,toIdxT)
      logger.info(s"search url: $url")

      cancellable = context.system.scheduler.scheduleOnce(10.seconds,context.parent,TimeoutFromHttp(fromIdxT))

      val rq = HttpRequest(HttpMethods.GET, uri = url)
      sendTo(io).withResponsesReceivedBy(self)(rq)
    }

    case HttpResponse(s,e,h,p) if s.isSuccess => {
      cancellable.cancel()

      val body = e.asString(HttpCharsets.`UTF-8`)

      if(body.forall(_.isWhitespace)) {
        logger.info(s"Couldn't fined any results between $from and $to sending NothingToSync")
        context.parent ! NothingToSync
      } else {
        var maxTime = 0L
        var minTime = Long.MaxValue
        val uuids = (Set.empty[String] /: body.trim.lines) {
          case (uuidSet,tsvLine) =>
            val (indexTime,uuid) = extractIndexTimeAndUuid(tsvLine)
            if(indexTime > maxTime) maxTime = indexTime
            if(indexTime < minTime) minTime = indexTime
            uuidSet + uuid
        }
        val rFrom =
          if(from == 0L) minTime
          else from

        if(uuids.size < minChunkSize) {
          logger.info(s"The refined range is ($rFrom - $to), same as original range is ($from - $to) because uuids.size = ${uuids.size}")
          context.parent ! RefinedSyncRange(dc, rFrom -1, to, uuids)
        }
        else {
          if(uuids.size < maxChunkSize) {
            logger.info(s"not maximal batch: uuids.size = ${uuids.size}")
          }
          logger.info(s"The refined range is ($rFrom - ${maxTime}), original range is ($from - $to)")
          context.parent ! RefinedSyncRange(dc, rFrom -1, maxTime + 1, uuids)
        }
      }
    }
    case unknown => logger.warn(s"unknown message: $unknown")
  }
}

case object ScanDataCenters

object DataCenterSyncControler {
  lazy val ref = Grid.serviceRef("DataCenterSyncManager")
}

class DataCenterSyncControler(dstServersVec : Vector[String]) extends Actor with LazyLogging {

  var dataCenterActorMap : Map [String,ActorRef] = Map.empty[String,ActorRef]
  var dataCenterIdSet  : Set [String] = Set.empty[String]
  def randomDestHost = dstServersVec(scala.util.Random.nextInt(dstServersVec.size))

  override def preStart : Unit = {
    logger.info("Starting DataCenterSyncControler instance on this machine")
    self ! ScanDataCenters
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case e: Throwable => {
      logger.error("caught exception as supervisor",e)
      val dcSync = sender()
      val dataCenterId = dataCenterActorMap.find(_._2 == dcSync).map(_._1)
      dataCenterId.foreach { id =>
        dataCenterIdSet -= id
        dataCenterActorMap -= id
      }
      Stop
    }
  }

  def receive: Receive = GridReceives.monitoring(sender) orElse {
    case ScanDataCenters =>
      logger.info("ScanDataCenters")
      val a = context.actorOf(Props(new RetrieveDcList(randomDestHost)))
      a ! Retrieve
    case DcData(s) =>
      // so we do not have actor leak (a memory leak)
      sender ! PoisonPill
      if (s.isEmpty) {
        logger.info("DcData is empty.")
        context.system.scheduler.scheduleOnce(10.seconds , self, ScanDataCenters)
      } else {

        logger.info(s"DcData has data. [$s]")
        val onlyRemoteDc = s.filter( d =>  d.t == Remote )
        onlyRemoteDc.foreach{
          d => if ( !dataCenterIdSet(d.dataCenterId) ) {
            logger.info(s"${d.dataCenterId} is not in the set we are adding it.")
            // if it is not in the set we need to create actor and add it to the list
            dataCenterIdSet += d.dataCenterId
            logger.info(s"create actor with ${d.location} | ${dstServersVec.mkString("[",",","]")} | ${d.dataCenterId}")
            val dcSync = context.actorOf(Props(new DcSync(d.location , dstServersVec , d.dataCenterId)))
            dataCenterActorMap += d.dataCenterId -> dcSync

            val idxTimeRetriever = context.actorOf(Props( new LastIndexTimeRetriver(randomDestHost,d.dataCenterId)))
            idxTimeRetriever ! Retrieve
          }
        }
        context.system.scheduler.scheduleOnce(60.seconds , self, ScanDataCenters)
      }
    case LocalStatusForDC(dataCenterId, fromTime) => {
      // so we do not have actor leak (a memory leak)
      sender ! PoisonPill
      val from = math.max(fromTime - Settings.overlap, 0L)
      dataCenterActorMap(dataCenterId) ! SyncData(from)
    }
    case LocalErrorStatusForDC(dataCenterId, err) => {
      // so we do not have actor leak (a memory leak)
      sender ! PoisonPill
      dataCenterActorMap(dataCenterId) ! PoisonPill
      logger.error(err)
      dataCenterIdSet -= dataCenterId
      dataCenterActorMap -= dataCenterId
    }
  }
}

object DcMain extends App with LazyLogging {
  import Settings._

  Grid.declareServices(ServiceTypes().
    add("DataCenterSyncManager", classOf[DataCenterSyncControler], target).add(HealthControl.services)
  )

  Grid.setGridConnection(GridConnection(memberName = "dc"))
  Grid.joinClient

  HealthControl.init
}


object DcMainStandAlone extends App with LazyLogging {
  import Settings._
  Grid.setGridConnection(GridConnection(memberName = "DC", clusterName = "dc-stand-alone", hostName = "127.0.0.1", port = 9988, seeds = Set("127.0.0.1:9988")))
  Grid.join
  Thread.sleep(10000)
  val dcSync = Grid.createSingleton( classOf[DataCenterSyncControler], "DcController", Some("DcClient") , target )

}


//object CopyDc extends App {
//
//  implicit val system = ActorSystem("copy-dc")
//
//  def buildStreamUrl(srcHostName : String , dataCenterId : String ,fromTime : Long  , toTime : Long ) : String = s"http://$srcHostName/?op=${Settings.streamOp}&with-history&with-descendants&qp=-system.parent.parent_hierarchy:/meta/,system.dc:$dataCenterId,system.indexTime>$fromTime,system.indexTime<$toTime,system.lastModified>1970&format=jsonldq&with-meta"
//
//  def buildPushUrl(dstHostName : String ) : String = s"http://$dstHostName/_ow?format=trig"
//
//  def shutdown(): Unit = {
////    IO(Http).ask(Http.CloseAll)(1.second).await
//    system.shutdown()
//  }
//
//  system.actorOf(Props(classOf[DataCenterSyncControler], Settings.target.split(",").toSet))
//}
