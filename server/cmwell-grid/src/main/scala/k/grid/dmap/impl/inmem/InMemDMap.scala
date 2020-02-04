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
package k.grid.dmap.impl.inmem

import k.grid.dmap.api.{DMapActorInit, DMapFacade, DMapMaster, DMapSlave}

/**
  * Created by michael on 5/29/16.
  */
object InMemDMap extends DMapFacade {
  override def masterType: DMapActorInit = DMapActorInit(classOf[InMemMaster], "InMemMaster")

  override def slaveType: DMapActorInit = DMapActorInit(classOf[InMemSlave], "InMemSlave")
}

class InMemMaster extends DMapMaster {
  override val facade: DMapFacade = InMemDMap
}

class InMemSlave extends DMapSlave {
  override val facade: DMapFacade = InMemDMap
}
