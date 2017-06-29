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
import cmwell.util.string.Hash.adler32long
import spray.client.pipelining._
import spray.http._

import scala.collection.mutable.{PriorityQueue, Queue => MQueue}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Created by gilad on 10/13/15.
 */
class PushManager(destinationHosts: Vector[String], httpRequestsManager: ActorRef, dc: String) extends Actor with cmwell.dc.LazyLogging {

  val pushers: Vector[ActorRef] = destinationHosts.map(host => context.actorOf(Props(classOf[PushToHostWithQueue],host, dc,httpRequestsManager, context.parent)))

  override def receive: Receive = {
    case ManagedPushMsg(nquads,bulk) => {
      logger.debug(s"PushManager: got ManagedPushMsg(nquads = ${nquads.size}, bulk = ${bulk.size})")
      val subToLines = nquads.groupBy(_.trim.split(' ')(0).tail.init)
      subToLines.foreach{
        case (subject,lines) => {
          val pusher = pushers((adler32long(subject) % pushers.size).toInt)
          val imd = bulk.find(imd => lines.exists(_.contains(imd.uuid))) match {
            case Some(x) => x
            case None => {
              logger.error(s"None as imd (not found in bulk, subject = '${subject}', lines = \n${lines.mkString("\n")}\n\n bulk = ${bulk.map(_.uuid).mkString("[",",","]")})")
              InfotonMetaData(subject,subject,0L)
            }
          }
          val noRedundantMeta = lines.filterNot(stmt => Set("uuid","parent","path").exists(att => stmt.contains(s"/meta/sys#$att")))
          pusher ! AppendToQueue(noRedundantMeta, imd)
        }
      }
    }
    case unknown => {
      logger.error(s"unknown message received: $unknown")
    }
  }
}

/* ******** *
 *          *
 * messages *
 *          *
 * ******** */
case class ManagedPushMsg(nquads: Seq[String], bulk: Set[InfotonMetaData])
case class AppendToQueue(nquads: Seq[String], imd: InfotonMetaData)
case object FlushQueue

/* ******************* *
 *                     *
 * PushToHostWithQueue *
 *                     *
 * ******************* */
object PushToHostWithQueue {
  //TODO: validate lt vs. gt behavior
  val ord: Ordering[(Seq[String],InfotonMetaData)] =
    Ordering.fromLessThan((a,b)=> a._2.indexTime < b._2.indexTime)
}

class PushToHostWithQueue(host: String, dc: String, httpRequestsManager: ActorRef, altSyncronizer: ActorRef) extends Actor with cmwell.dc.LazyLogging {
  val pQueue = PriorityQueue.empty[(Seq[String],InfotonMetaData)](PushToHostWithQueue.ord)
  var cancellable: Option[Cancellable] = None

  def takeBulkFromQueue: (Seq[String],Set[InfotonMetaData]) = {
    ((Seq.empty[String] -> Set.empty[InfotonMetaData]) /: (1 to Settings._owBulkSize)){ case (pair@(xs,imds),_) =>
      //making sure different versions of same infoton won't get merged after concatenating nquads lines
      if(imds.exists(imd => pQueue.headOption.exists(_._2.path == imd.path))) pair
      else {
        val (ys,imd) = pQueue.dequeue()
        (xs ++ ys) -> (imds + imd)
      }
    }
  }

  override def receive: Receive = {
    case AppendToQueue(nquads,imd) => {
//      logger.debug(s"AppendToQueue(nquads=${nquads.size},imd=[${imd.path},${imd.uuid},${imd.indexTime}}]})")
      pQueue.enqueue(nquads -> imd)

      if(pQueue.size >= Settings._owBulkSize) {
        cancellable.foreach(_.cancel())
        cancellable = None
        do {
          val (nquads, bulk) = takeBulkFromQueue
          logger.trace(s"creating PushNQActor(${nquads.size},${bulk.size},$host,$dc,$httpRequestsManager)")
          context.actorOf(Props(classOf[PushNQActor], nquads, bulk, host, dc, httpRequestsManager))
        } while (pQueue.size >= Settings._owBulkSize)
      }
      if(pQueue.nonEmpty && cancellable.isEmpty)
        cancellable = Some(context.system.scheduler.scheduleOnce(1.seconds, self, FlushQueue))
    }

    case FlushQueue => {
      cancellable.foreach(_.cancel())
      cancellable = None
      val (nquads, bulk) = {
        if(pQueue.size >= Settings._owBulkSize) {
          val rv = takeBulkFromQueue
          if(pQueue.nonEmpty && cancellable.isEmpty) {
            cancellable = Some(context.system.scheduler.scheduleOnce(1.seconds, self, FlushQueue))
          }
          rv
        }
        else {
          val (stmts,imds) = pQueue.dequeueAll.unzip
          stmts.flatten -> imds.toSet
        }
      }
      context.actorOf(Props(classOf[PushNQActor], nquads, bulk, host, dc, httpRequestsManager))
    }
    case msg @ (_: PushedWithInfotonsMetaData | _: FailedToPushNquadsWithInfotonMetaData) => {
      sender() ! PoisonPill
      altSyncronizer ! msg
    }
  }
}

/* *********** *
 *             *
 * PushNQActor *
 *             *
 * *********** */
class PushNQActor(nquads: Seq[String], bulk: Set[InfotonMetaData], dst: String, override val dc: String, io: ActorRef) extends Actor with cmwell.dc.LazyLogging with DCFixer {

  var payload: String = nquads.mkString("\n")

  logger.trace(s"PushNQActor> Payload just before POSTing (total of ${bulk.size}) =${nquads.mkString("\n\t","\n\t","\n")}")

  def req = {
    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)` ,payload)
    val uri = s"http://$dst/_ow?format=nquads"
    //temp token for testing purposes valid until 2016-01-17T14:16:37.528+02:00
    HttpRequest(HttpMethods.POST, uri = uri, entity = entity, headers = List(HttpHeaders.RawHeader("X-CM-WELL-TOKEN","""the dca token should be here but this is a dead code""")))
  }

  override def preStart(): Unit = {
    sendTo(io).withResponsesReceivedBy(self)(req)
  }

  def receive: Receive = retry(0)

  def retry(count: Int): Receive = {
    case ManagedResponse(Success(r@HttpResponse(s,_,_,_))) if s.isSuccess => {
      logger.trace("PushNQActor: recieved success")
      payload = null //big string, let's help GC
      context.parent ! PushedWithInfotonsMetaData(r,bulk)
    }
    case ManagedResponse(Success(r@HttpResponse(s,_,_,_))) if s.isFailure && count > Settings.maxRetries => {
      logger.debug("PushNQActor: exceeded max retries")
      payload = null //big string, let's help GC
      context.parent ! FailedToPushNquadsWithInfotonMetaData(nquads, bulk)
    }
    case ManagedResponse(Success(r@HttpResponse(s,_,_,_))) if s.isFailure && s.intValue < 500 => {
      logger.warn(s"PushNQActor: Failed to push $r")
      context.parent ! FailedToPushNquadsWithInfotonMetaData(nquads, bulk)
      payload = null //big string, let's help GC
    }
    case ManagedResponse(Success(r@HttpResponse(s,_,_,_))) if s.isFailure => {
      logger.error("PushNQActor: got failed response")
      context.system.scheduler.scheduleOnce((5*(count + 1)).seconds, self, TryAgain)
    }
    case ManagedResponse(Failure(e)) => {
      logger.error("PushNQActor: got Failure", e)
      context.system.scheduler.scheduleOnce(30.seconds,io,req)
    }
    case TryAgain => {
      io ! req
      context.become(retry(count+1))
    }
  }
}