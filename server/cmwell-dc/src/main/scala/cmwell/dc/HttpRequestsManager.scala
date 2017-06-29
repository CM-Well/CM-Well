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

import akka.actor.{ActorRef, Actor}
import akka.io.IO
import akka.util.Timeout
import spray.can.Http
import spray.http.{HttpResponse, HttpRequest}
import spray.client.pipelining._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.immutable.Queue
import scala.util._

/**
 * Created by gilad on 10/12/15.
 */
class HttpRequestsManager(sendLimit: Int) extends Actor with cmwell.dc.LazyLogging {

  case object Dequeue
  case class RefAndResponse(ref: ActorRef, res: Try[HttpResponse])

  val io = IO(Http)(context.system)

  override def receive: Receive = receiveWithQueue(Queue.empty[(ActorRef,HttpRequest)],0,sentDequeueToSelf = false)

  def receiveWithQueue(requests: Queue[(ActorRef,HttpRequest)], sent: Int, sentDequeueToSelf: Boolean): Receive = {
    case req: HttpRequest => {
      if(sent < sendLimit && !sentDequeueToSelf) {
        self ! Dequeue
        context.become(receiveWithQueue(requests.enqueue(sender() -> req), sent, sentDequeueToSelf = true))
      }
      else {
        context.become(receiveWithQueue(requests.enqueue(sender() -> req), sent, sentDequeueToSelf))
      }
    }
    case Dequeue if requests.nonEmpty => {
      var sCount = sent
      var newQueue = requests
      while(sCount < sendLimit && newQueue.nonEmpty) {
        val ((aRef, req), q) = newQueue.dequeue
        sendReceive(io)(global, Timeout(60.seconds))(req).onComplete { t =>
          self ! RefAndResponse(aRef, t)
        }
        newQueue = q
        sCount += 1
      }
      context.become(receiveWithQueue(newQueue, sCount, sentDequeueToSelf = false))
    }
    case Dequeue if requests.isEmpty => //Nothing ;)
    case RefAndResponse(ref,res) => {
      ref ! ManagedResponse(res)
      if(requests.nonEmpty && !sentDequeueToSelf) {
        self ! Dequeue
        context.become(receiveWithQueue(requests,sent-1,sentDequeueToSelf = true))
      }
      else {
        context.become(receiveWithQueue(requests,sent-1,sentDequeueToSelf = false))
      }
    }
  }
}

case class ManagedResponse(response: Try[HttpResponse])
