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
package k.grid.registration

import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.pattern.ask
import scala.concurrent.duration._
import com.typesafe.scalalogging.LazyLogging
import k.grid.{Grid, GridJvm}
import k.grid.registration.messages.{GridTopology, RegistrationPing}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 3/21/16.
  */
object RegistrationCoordinator {
  val name = "GossipCoordinator"

  def init = {
    Grid.createSingleton(classOf[RegistrationCoordinator], RegistrationCoordinator.name, None)
    Grid.selectSingleton(RegistrationCoordinator.name, None)
  }

}
class RegistrationCoordinator extends Actor with LazyLogging {
  private case class GridJvmContainer(gj: GridJvm, ts: Long)
  private case object ClearIdles

  private def currentSeconds = System.currentTimeMillis() / 1000
  private[this] var jvmSet = Set.empty[GridJvmContainer]
  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    context.system.scheduler.schedule(0.seconds, 5.seconds, self, ClearIdles)
  }

  override def receive: Receive = {
    case RegistrationPing(jvm) =>
      jvmSet = jvmSet + GridJvmContainer(jvm, currentSeconds)
      sender ! GridTopology(jvmSet.map(_.gj))
    case ClearIdles =>
      val currentTime = currentSeconds
      jvmSet = jvmSet.filter(jvm => currentTime - jvm.ts < 30)
  }
}
