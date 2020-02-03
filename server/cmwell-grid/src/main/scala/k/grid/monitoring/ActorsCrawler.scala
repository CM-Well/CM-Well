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
package k.grid.monitoring

import akka.actor._
import k.grid.Grid

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by michael on 7/5/15.
  */
case object PingActors
case class ActorReport(name: String, latency: Long = 0L)
case class ActiveActors(actors: Set[ActorReport], host: String = "")
case object CheckIfFinished
class ActorsCrawler extends Actor {

  private[this] var s = Set.empty[ActorReport]
  private[this] var senderVar: ActorRef = _
  private[this] var lastUpdate: Long = Long.MaxValue
  private[this] def currentTime = System.currentTimeMillis

  override def receive: Receive = {
    case ActorIdentity(cId, refO) =>
      refO match {
        case Some(ref) =>
          val sentIn = cId.asInstanceOf[Long]
          s = s + ActorReport(ref.path.toString, currentTime - sentIn)
          lastUpdate = currentTime
          context.system.actorSelection(ref.path / "*") ! Identify(currentTime)
        case None =>
      }

    case CheckIfFinished =>
      if (currentTime - lastUpdate > 1000) {
        senderVar ! ActiveActors(s, Grid.thisMember.address)
        self ! PoisonPill
      }

    case PingActors =>
      senderVar = sender()
      context.system.actorSelection("/user/*") ! Identify(currentTime)
      context.system.scheduler.schedule(0.seconds, 1.seconds, self, CheckIfFinished)
  }
}
