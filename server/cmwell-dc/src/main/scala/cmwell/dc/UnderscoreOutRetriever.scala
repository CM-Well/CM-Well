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

import akka.actor._
import spray.can.Http.{ConnectionAttemptFailedException, ConnectionException}
import spray.http._

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Created by gilad on 9/20/15.
 */
class UnderscoreOutRetrieveManager(src: String, override val dc: String, reqMgr: ActorRef) extends Actor with DCFixer {

  val bulks = MMap.empty[ActorRef,Set[InfotonMetaData]]

  override def receive: Receive = {
    case SyncChunk(chunk) => {
      logger.debug(s"SyncChunk(${chunk.size} uuids)")
      for {
        bulk <- chunk.grouped(Settings._outBulkSize)
        aRef = context.actorOf(Props(classOf[UnderscoreOutRetriever],src,bulk,reqMgr))
      } {
        bulks.update(aRef, bulk)
        aRef ! Retrieve
      }
    }
    case FailedPull => {
      val s = sender()
      s ! PoisonPill
      val bulkOpt = bulks.remove(s)
      bulkOpt match {
        case None => logger.error("fail to pull uuids, but can't specify since it's missing from bulks map")
        case Some(bulk) if bulk.size == 1 => logger.error(s"fail to pull following uuid: [${bulk.head.uuid}]")
        case Some(bulk) => {
          logger.warn(s"fail to pull following uuids: [${bulk.map(_.uuid).mkString(",")}], will try to retrieve as singles")
          for{
//            (imd,i) <- bulk.zipWithIndex
            imd <- bulk
            aRef = context.actorOf(Props(classOf[UnderscoreOutRetriever],src,Set(imd),reqMgr))
          } {
            bulks.update(aRef, Set(imd))
//            context.system.scheduler.scheduleOnce((i*100).millis,aRef,Retrieve)
            aRef ! Retrieve
          }
        }
      }
      if(bulkOpt.exists(_.size == 1)) context.parent ! FailedPullEvent(bulkOpt.get.head)
      if(bulks.isEmpty) context.parent ! DonePull
    }
    case NQuads(nquads) => {
      val s = sender()
      s ! PoisonPill

      val nq = NQuads(fixDcAndIndexTime(nquads))

      bulks.remove(s) match {
        case Some(bulk) => context.parent ! nq.withInfotonsMetaData(bulk)
        case None => {
          logger.warn("succeeded to pull nquads, but removal of actor ref failed")
          context.parent ! nq
        }
      }
      if(bulks.isEmpty) context.parent ! DonePull
    }
  }
}

class UnderscoreOutRetriever(host: String, bulk: Set[InfotonMetaData], reqMgr: ActorRef) extends Actor with NQuadsUtil {

  val retriesLimit = Settings.maxRetries

  val url = s"http://$host/_out?format=nquads"
  val uuids = bulk.map(_.uuid)
  val body = uuids.mkString("/ii/","\n/ii/","")
  val req = HttpRequest(HttpMethods.POST, uri = url, entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`,body))
  logger.info(s"retrieving ${uuids.size} uuids")

  override def receive: Receive = recieveWithRetries(0)

  def recieveWithRetries(tryNum: Int): Receive = {
    case Retrieve => reqMgr ! req
    case ManagedResponse(Success(res@HttpResponse(s, e, h, p))) if s.isSuccess => {

      logger.trace(s"UnderscoreOutRetriever> Payload just received: ${e.asString.lines.mkString("\n\t","\n\t","\n")}")

      val nquads = e.asString.lines.filterNot(_.startsWith("_")).toSeq
      val foundSubjects = groupBySubject(nquads).collect{
        case (Some(s),seq) => s -> seq
      }
      if(foundSubjects.size == bulk.size) {
        logger.debug(s"success. retrieved ${uuids.size} uuids")
        context.parent ! NQuads(nquads)
      }
      else {
        //TODO: we already have partial data... why not use it?
        logger.error(s"found only ${foundSubjects.size} infotons out of requested ${uuids.size} infotons")
        context.parent ! FailedPull
      }
    }
    case ManagedResponse(Success(res@HttpResponse(s, e, h, p))) if s.intValue < 500 => {
      //bad requests... no point to retry
      logger.error(s"bad response received: $res, sending FailedPull")
      context.parent ! FailedPull
    }
    case ManagedResponse(Success(unknown)) => {
      logger.error(s"received: $unknown, resending Retrieve")
      if(tryNum >= retriesLimit) context.parent ! FailedPull
      else {
        context.system.scheduler.scheduleOnce((10*(1+tryNum)).seconds, self, Retrieve)
        context.become(recieveWithRetries(tryNum+1))
      }
    }
    case ManagedResponse(Failure(_: akka.pattern.AskTimeoutException | _: ConnectionAttemptFailedException | _: ConnectionException)) => {
      logger.error(s"timeout for request (probably a bad network issue, will slow down for another retry), #retry=$tryNum")
      val waitTime = math.min(60,10 * (tryNum + 1))
      context.system.scheduler.scheduleOnce(waitTime.seconds, self, Retrieve)
      context.become(recieveWithRetries(tryNum + 1))
    }
    case ManagedResponse(Failure(error)) => {
      logger.error(s"error, resending Retrieve in 5 sec", error)
      if(tryNum >= retriesLimit) context.parent ! FailedPull
      else {
        context.system.scheduler.scheduleOnce((10*(1+tryNum)).seconds, self, Retrieve)
        context.become(recieveWithRetries(tryNum + 1))
      }
    }
  }
}