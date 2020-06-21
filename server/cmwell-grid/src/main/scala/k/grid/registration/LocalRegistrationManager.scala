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

import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.Actor.Receive
import com.typesafe.scalalogging.LazyLogging
import k.grid._
import k.grid.registration.messages.{GridTopology, RegistrationPing}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 3/21/16.
  */
object LocalRegistrationManager {
  val name = "LocalGossipManager"
  // todo: change to Map[(Host, IdentityName),GridJvm]
  private[LocalRegistrationManager] var _jvms = Set.empty[GridJvm]
  private[LocalRegistrationManager] var _regFails = 0

  def registrationFailure = _regFails == Config.possibleRegFails

  def jvms = _jvms
}
class LocalRegistrationManager extends Actor with LazyLogging {
  private case object SendGossipPing
  private case object IncreaseRegFails

  val isController = Grid.isController
  def registrationCoordinator = Grid.selectSingleton(RegistrationCoordinator.name, None, Grid.clusterProxy)
  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    context.system.scheduler.schedule(5.seconds, 5.seconds, self, SendGossipPing)
    context.system.scheduler.schedule(30.seconds, 30.seconds, self, IncreaseRegFails)

  }

  override def receive: Receive = {
    case SendGossipPing => registrationCoordinator ! RegistrationPing(Grid.thisMember)
    case GridTopology(jvms) =>
      LocalRegistrationManager._regFails = 0

      val jvmsJoined = jvms -- LocalRegistrationManager._jvms
      val jvmsLeft = LocalRegistrationManager._jvms -- jvms

      LocalRegistrationManager._jvms = jvms

      // send the data to the client actor so it can forward it to its subscribers.
      Grid.selectActor(ClientActor.name, Grid.thisMember) ! JvmMembershipReport(jvmsJoined, jvmsLeft)

      logger.debug(s"Current jvms: $jvms")
    case IncreaseRegFails =>
      LocalRegistrationManager._regFails = Math.min(LocalRegistrationManager._regFails + 1, Config.possibleRegFails)
  }
}
