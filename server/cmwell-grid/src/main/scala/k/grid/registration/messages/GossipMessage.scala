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
package k.grid.registration.messages

import k.grid.GridJvm

/**
  * Created by michael on 3/21/16.
  */
trait GossipMessage

case class RegistrationPing(jvm: GridJvm) extends GossipMessage
case class GridTopology(jvms: Set[GridJvm]) extends GossipMessage
