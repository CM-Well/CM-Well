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
package cmwell.ctrl.service

import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import cmwell.ctrl.hc.HealthActor
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.concurrent.duration._

/**
  * Created by michael on 2/1/16.
  */
class ClusterServiceActor extends Actor {
  implicit val timeout = Timeout(3 seconds)
  override def receive: Receive = {
    case msg => (HealthActor.ref ? msg).pipeTo(sender)
  }
}
