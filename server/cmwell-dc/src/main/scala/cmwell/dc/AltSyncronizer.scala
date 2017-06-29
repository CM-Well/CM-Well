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
import akka.actor.SupervisorStrategy.{Resume, Stop, Restart}
import akka.io.IO

import spray.can.Http
import spray.http._
import spray.client.pipelining._

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by gilad on 9/19/15.
 */
class AltSyncronizer(srcHostName: String, destinationHosts: Vector[String], dataCenterId: String) extends Actor with cmwell.dc.LazyLogging {

  val io = IO(Http)(context.system)
  val reqMgr = context.actorOf(Props(classOf[HttpRequestsManager], Settings.maxPullRequests))
  val tsvRetriever = context.actorOf(Props(new TsvRetriever(srcHostName,dataCenterId,io)))
  val pushManager = {
    val httpRequestsManager = context.actorOf(Props(classOf[HttpRequestsManager], Settings.maxPushRequests))
    context.actorOf(Props(new PushManager(destinationHosts,httpRequestsManager,dataCenterId)))
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case e: Throwable => {
      val s = sender()
      logger.error(s"AltSyncronizer child died ($s)",e)
      if(s != tsvRetriever) Restart
      else {
        self ! Kill
        Resume
      }
    }
  }

  override def receive: Receive = receiveWithReqCountAndCaches(0, Set.empty, Set.empty, Set.empty)

  def receiveWithReqCountAndCaches(uOutRetMngrsCount: Int,
                                   uuidsToRetrieve: Set[InfotonMetaData],
                                   retrievedUuids: Set[InfotonMetaData],
                                   pushedUuids: Set[InfotonMetaData]): Receive = {
    case StartSyncFrom(fromTime) => {
      logger.info(s"Working on $fromTime - now")
      tsvRetriever ! StartSyncFrom(fromTime)
    }
    case chunk@SyncChunk(imds) => {
      logger.debug(s"got SyncChunk(${imds.size}), sending to UnderscoreOutRetrieveManager (after filtering duplicates). uOutRetMngrsCount=${uOutRetMngrsCount+1}")
      val additions = imds.filterNot(imd => uuidsToRetrieve(imd) || retrievedUuids(imd) || pushedUuids(imd))
      if(additions.nonEmpty) {
        context.actorOf(Props(classOf[UnderscoreOutRetrieveManager], srcHostName, dataCenterId, reqMgr)) ! SyncChunk(additions)
        context.become(receiveWithReqCountAndCaches(uOutRetMngrsCount + 1, uuidsToRetrieve ++ additions, retrievedUuids, pushedUuids))
      }
      else {
        context.system.scheduler.scheduleOnce(10.seconds,tsvRetriever,GetChunk)
      }
    }
    case NQuadsWithInfotonMetaData(nquads,bulk) => {
      logger.trace(s"got NQuadsWithInfotonMetaData(nquads, ${bulk.size} uuids)")
      pushManager ! ManagedPushMsg(nquads,bulk)
      context.become(receiveWithReqCountAndCaches(uOutRetMngrsCount, uuidsToRetrieve &~ bulk, retrievedUuids | bulk, pushedUuids))
    }
    case nq@NQuads(nquads) => {

      val (retrieved,rest) = {
        val subToLines = nquads.groupBy{ line =>
          line.trim.split(' ')(0)
        }
        val uuids = subToLines.map{
          case (s,xs) => {
            val uOpt = xs.find(_.contains("/meta/sys#uuid>"))
            val uuid = uOpt.map(_.trim.split(' ')(2).tail.init) match {
              case Some(u) => u
              case None => {
                logger.error(s"could'nt find uuid in ${s}")
                "NO_UUID-" + s.tail.init
              }
            }
            uuid
          }
        }.toSet

        uuidsToRetrieve.partition(imd => uuids(imd.uuid))
      }

      logger.info(s"got NQuads(nquads with ${retrieved.size + rest.size} uuids)")
      pushManager ! ManagedPushMsg(nquads,retrieved)
      context.become(receiveWithReqCountAndCaches(uOutRetMngrsCount, rest, retrievedUuids | retrieved, pushedUuids))
    }
    case FailedPullEvent(imd) => {
      logger.error(s"could not pull $imd")
      redlog.error(s"pull failed for uuid: ${imd.uuid}")
      context.become(receiveWithReqCountAndCaches(uOutRetMngrsCount, uuidsToRetrieve - imd, retrievedUuids, pushedUuids))
      if(uOutRetMngrsCount < 2 && retrievedUuids.size < 2*Settings.uuidsCacheSize) {
        logger.info(s"requesting GetChunk, uOutRetMngrsCount=$uOutRetMngrsCount")
        tsvRetriever ! GetChunk
      }
    }
    case DonePull => {
      logger.debug(s"killing UnderscoreOutRetrieveManager instance (uOutRetMngrsCount=$uOutRetMngrsCount)")
      sender() ! PoisonPill
      context.become(receiveWithReqCountAndCaches(uOutRetMngrsCount - 1, uuidsToRetrieve, retrievedUuids, pushedUuids))
      logger.info(s"requesting GetChunk, uOutRetMngrsCount=${uOutRetMngrsCount-1}")
      tsvRetriever ! GetChunk
    }
    case PushedWithInfotonsMetaData(res, bulk) => {

      logger.info(s"pushed $res")

      val newPushedToKeepCache = {
        val now = System.currentTimeMillis()
        if(pushedUuids.size < Settings.uuidsCacheSize) pushedUuids | bulk
        else {
          var oldToKeep = pushedUuids
          while(oldToKeep.size > Settings.uuidsCacheSize){
            val oldest = oldToKeep.minBy(_.indexTime)
            oldToKeep -= oldest
          }
          oldToKeep | bulk
        }
      }

      val remainingRetrieved = retrievedUuids &~ bulk
      context.become(receiveWithReqCountAndCaches(uOutRetMngrsCount, uuidsToRetrieve, remainingRetrieved, newPushedToKeepCache))
      logger.debug(s"after push, caches state: uuidsToRetrieve = ${uuidsToRetrieve.size}, retrievedUuids = ${remainingRetrieved.size}, pushedUuids = ${newPushedToKeepCache.size}")
      if(uOutRetMngrsCount < 2 && remainingRetrieved.size < 2*Settings.uuidsCacheSize) {
        logger.info(s"requesting GetChunk, uOutRetMngrsCount=$uOutRetMngrsCount")
        tsvRetriever ! GetChunk
      }
    }
    case FailedToPushNquadsWithInfotonMetaData(nquads, bulk) => {

      logger.error(s"failed to push data: $bulk")

      if(bulk.size > 1) {
        logger.info("sending bad batch as single infoton requests!")
        val groupedBySubject = nquads.groupBy(_.trim.split(' ').head)
        groupedBySubject.foreach{
          case (_,lines) => self ! NQuads(lines.toSeq).withInfotonsMetaData(bulk.filter{ imd =>
            lines.exists(_.contains(imd.uuid))
          })
        }
      }
      else {
        redlog.error("push failed on data:\n" + nquads)
      }
      val remainingRetrieved = retrievedUuids &~ bulk
      context.become(receiveWithReqCountAndCaches(uOutRetMngrsCount, uuidsToRetrieve, remainingRetrieved, pushedUuids))
      if(uOutRetMngrsCount < 2 && remainingRetrieved.size < 2*Settings.uuidsCacheSize) {
        logger.info(s"requesting GetChunk, uOutRetMngrsCount=$uOutRetMngrsCount")
        tsvRetriever ! GetChunk
      }
    }
  }
}

/* ******** *
 *          *
 * messages *
 *          *
 * ******** */

case object TryAgain
case object GetChunk
case object DonePush
case object FailedPull
case object DonePull
case object RetrieveTimeout
case class FailedPullEvent(imd: InfotonMetaData)
case class NQuadsWithInfotonMetaData(nquads: Seq[String], bulk: Set[InfotonMetaData])
case class NQuads(nquads: Seq[String]) {
  def withInfotonsMetaData(bulk: Set[InfotonMetaData]) = NQuadsWithInfotonMetaData(nquads,bulk)
}
case class PushedWithInfotonsMetaData(res: HttpResponse, bulk: Set[InfotonMetaData])
case class StartSyncFrom(from: Long)
case class SyncChunk(chunk: Set[InfotonMetaData])
case class FailedToPushNquadsWithInfotonMetaData(nquads: Seq[String], bulk: Set[InfotonMetaData])
case class InfotonMetaData(path: String, uuid: String, indexTime: Long)

