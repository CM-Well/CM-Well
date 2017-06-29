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

import akka.actor.{Cancellable, Actor, ActorRef}
import akka.util.Timeout
import akka.pattern.pipe
import spray.client.pipelining._
import spray.http.{HttpCharsets, HttpRequest, HttpResponse, HttpMethods}

import scala.collection.immutable.Queue
import scala.collection.mutable.{Set => MSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.Breaks.{break,breakable}

/**
 * Created by gilad on 9/19/15.
 */

class TsvRetriever(remote : String, dc: String, io: ActorRef) extends Actor with cmwell.dc.LazyLogging {
  val minChunkSize = Settings.minInfotonsPerBufferingSync
  val maxChunkSize = Settings.maxInfotonsPerBufferingSync
  val maxBuffers = Settings.maxBuffers
  var cancellableOpt: Option[Cancellable] = None

  def toTime = System.currentTimeMillis() - 30000L
  def searchUrl(from: Long) = s"http://$remote/?op=search&qp=-system.parent.parent_hierarchy:/meta/,system.dc::$dc,system.indexTime>$from,system.indexTime<$toTime,system.lastModified>1970&with-descendants&format=tsv&with-history&sort-by=*system.indexTime&length=$maxChunkSize"

  def extractTsvData(line: String): InfotonMetaData = {
    val arr = line.trim.split("\t")
    val indexTime = {
      if (arr.length == 4) arr(3).toLong
      else arr(1).toLong // TODO: jodatime string to long real convert
    }
    InfotonMetaData(arr(0), arr(2), indexTime)
  }

  override def receive: Receive = {
    case StartSyncFrom(from) => {
      context.become(receiveWithBuffersQueue(Queue(MSet.empty[InfotonMetaData]), from, waitingForReply=true, syncInProgress=false))
      self ! Sync
    }
    case unknown => logger.warn(s"unknown message received: $unknown")
  }

  def receiveWithBuffersQueue(buffers: Queue[MSet[InfotonMetaData]], from: Long, waitingForReply: Boolean, syncInProgress: Boolean): Receive = {
    case GetChunk if buffers.nonEmpty && buffers.head.nonEmpty => {
      val (s,q) = buffers.dequeue
      context.parent ! SyncChunk(s.toSet)
      context.become(receiveWithBuffersQueue(q,from,waitingForReply=false,syncInProgress))
      if(!syncInProgress && cancellableOpt.isEmpty) {
        logger.debug(s"sending Sync; case GetChunk if buffers.nonEmpty: from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
        self ! Sync
      }
    }
    case GetChunk => {
      if(!syncInProgress && cancellableOpt.isEmpty) {
        cancellableOpt = Some(context.system.scheduler.scheduleOnce(10.seconds,self,Sync))
        logger.debug(s"sending Sync in 10 sec; case GetChunk: from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
      }
      context.become(receiveWithBuffersQueue(buffers, from, waitingForReply = true, syncInProgress))
    }
    case RetrySync => {
      context.become(receiveWithBuffersQueue(buffers, from, waitingForReply, syncInProgress=false))
      logger.debug(s"sending Sync; case RetrySync: from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
      self ! Sync
    }
    case Sync if syncInProgress => logger.debug("sync is still in progress thus doing nothing.")
    case Sync => {
      cancellableOpt.foreach(_.cancel())
      cancellableOpt = None
      val url = searchUrl(from)
      logger.info(s"search url: $url")

      val rq = HttpRequest(HttpMethods.GET, uri = url)
      sendReceive(io)(global,Timeout(60.seconds))(rq).onComplete {
        case Success(res@HttpResponse(s,_,_,_)) if s.isSuccess =>
          logger.debug(s"received: tsv content from: $url")
          logger.trace(res.entity.asString.split("\n").map(ln => s"received tsv content: $ln").mkString("\n"))
          self ! res
        case Success(res: HttpResponse) if res.status.intValue == 504 => {
          logger.error("504 GATEWAY TIMEOUT")
          logger.debug(s"sending RetrySync in 10 sec; case Success(res: HttpResponse) if res.status.intValue == 504: from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
          context.system.scheduler.scheduleOnce(10.seconds, self, RetrySync)
        }
        case Success(res: HttpResponse) => {
          logger.error(s"bad http response recieved: $res")
          logger.debug(s"sending RetrySync in 10 sec; case Success(res: HttpResponse): from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
          context.system.scheduler.scheduleOnce(10.seconds, self, RetrySync)
        }
        case Success(unknown) => {
          logger.error(s"unknown message received: $unknown, resending Sync")
          self ! RetrySync
        }
        case Failure(e: akka.pattern.AskTimeoutException) => {
          logger.error("timeout for request (probably a bad network issue, will continue at slow pace until issue is resolved)",e)
          logger.debug(s"sending RetrySync in 10 sec; case Failure(e: akka.pattern.AskTimeoutException): from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
          context.system.scheduler.scheduleOnce(10.seconds, self, RetrySync)
        }
        case Failure(err) => {
          logger.error("http failure", err)
          logger.debug(s"sending RetrySync in 10 sec; case Failure(err): from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
          context.system.scheduler.scheduleOnce(10.seconds, self, RetrySync)
        }
      }
      context.become(receiveWithBuffersQueue(buffers, from, waitingForReply, syncInProgress=true))
    }
    case HttpResponse(s, e, h, p) => {
      val body = e.asString(HttpCharsets.`UTF-8`)
      if (body.forall(_.isWhitespace) && (buffers.isEmpty || buffers.forall(_.isEmpty))) {
        logger.info(s"Couldn't find any results from $from until current time, sending NothingToSync")
        context.become(receiveWithBuffersQueue(Queue(MSet.empty[InfotonMetaData]), from, waitingForReply, syncInProgress = false))
        cancellableOpt match {
          case None => {
            cancellableOpt = Some(context.system.scheduler.scheduleOnce(10.seconds, self, Sync))
            logger.debug(s"sending Sync in 10 sec; case HttpResponse: from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
          }
          case _ => //DO NOTHING, already about to receive message from scheduler
        }
      }
      else {
        var buffersToBecomeOn = buffers
        var continueToBuffer = true
        val lines = body.trim.lines.toSeq
        breakable {
          lines.foreach { tsvLine =>
            val imd = extractTsvData(tsvLine)
            if (!buffersToBecomeOn.exists(_.exists(_.uuid == imd.uuid))) {
              buffersToBecomeOn.find { buff =>
                buff.size <= maxChunkSize && !buff.exists(_.path == imd.path)
              } match {
                case Some(buffer) => buffer.add(imd)
                case None if buffersToBecomeOn.size < maxBuffers => buffersToBecomeOn = buffersToBecomeOn.enqueue(MSet(imd))
                case _ => {
                  continueToBuffer = false
                  break()
                }
              }
            }
          }
        }
        var newFrom = from
        val backStep = {
          //regular flow
          if(lines.size < minChunkSize && from > Settings.overlap) Settings.overlap
          //regular flow on a thin environment with very little infotons in it
          else if(lines.size < minChunkSize) Settings.overlap - from + 1
          //catching up on a heavy environment
          else 1
        }
        buffersToBecomeOn.foreach(_.foreach {
          case InfotonMetaData(_, _, indexTime) if (indexTime - 1) > newFrom => newFrom = indexTime - backStep
          case _ => //DO NOTHING
        })

        if(waitingForReply) {
          val (s,q) = buffersToBecomeOn.dequeue
          logger.debug(s"sending Sync; case HttpResponse: newFrom=$newFrom, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
          self ! Sync
          context.parent ! SyncChunk(s.toSet)
          context.become(receiveWithBuffersQueue(q, newFrom, waitingForReply=false, syncInProgress=false))
        }
        else {

          if(continueToBuffer && buffersToBecomeOn != buffers && cancellableOpt.isEmpty) {
            logger.debug(s"sending Sync; case HttpResponse: body not empty && continueToBuffer=$continueToBuffer, newFrom=$newFrom, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
            self ! Sync
          }
          else if(continueToBuffer && cancellableOpt.isEmpty) {
            cancellableOpt = Some(context.system.scheduler.scheduleOnce(10.seconds, self, Sync))
            logger.debug(s"sending Sync in 10 sec; case HttpResponse: continueToBuffer=$continueToBuffer, from=$from, waitingForReply=$waitingForReply, syncInProgress=$syncInProgress")
          }
          context.become(receiveWithBuffersQueue(buffersToBecomeOn, newFrom, waitingForReply, syncInProgress=false))
        }
      }
    }
  }
}

case object Sync
case object RetrySync