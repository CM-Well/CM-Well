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
package k.grid.service.messages

import akka.actor.Actor
import k.grid.GridJvm

/**
  * Created by michael on 2/10/16.
  */
trait ServiceUtilMessage

case class ServiceStatus(name: String, isRunning: Boolean, preferredJVM: Option[GridJvm])

case class ServiceInstantiationRequest(member: GridJvm, s: Set[ServiceStatus]) extends ServiceUtilMessage
case class RunService(name: String) extends ServiceUtilMessage
case class StopService(name: String) extends ServiceUtilMessage
case class ServiceMapping(mapping: Map[String, Option[GridJvm]]) extends ServiceUtilMessage

case class RegisterServices(gm: GridJvm) extends ServiceUtilMessage
case object SendInstantiationRequest extends ServiceUtilMessage
