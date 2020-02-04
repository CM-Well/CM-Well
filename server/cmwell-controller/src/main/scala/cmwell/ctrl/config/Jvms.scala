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
package cmwell.ctrl.config

import k.grid.{JvmIdentity, NodeMember}

/**
  * Created by michael on 2/4/16.
  */
object Jvms {
  def node = CTRL

  val CTRL = JvmIdentity("ctrl", 7777, Set("CTRL", "ControllerServer"))
  val WS = JvmIdentity("ws", 0)
  val BG = JvmIdentity("bg", 0)
  val DC = JvmIdentity("dc", 0)
  val CW = JvmIdentity("cw", 2561)

  val roles: Set[JvmIdentity] = Set(CTRL, WS, BG, DC, CW)
}
