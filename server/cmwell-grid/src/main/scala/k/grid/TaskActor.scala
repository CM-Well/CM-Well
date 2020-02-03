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
package k.grid

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, Cancellable}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

/**
  * Created by michael on 12/30/14.
  */
case object PauseTask
case object ResumeTask
class TaskActor(start: Long, interval: Long, block: () => Unit) extends Actor with ActorLogging {

  var s: Cancellable = _

  def startTask: Cancellable = {
    Grid.system.scheduler.schedule(start milli, interval milli) {
      block()
    }
  }

  s = startTask

  override def receive: Receive = {
    case PauseTask  => if (!s.isCancelled) s.cancel()
    case ResumeTask => if (s.isCancelled) s = startTask
  }
}
