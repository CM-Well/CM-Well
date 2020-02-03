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
package cmwell.ctrl.controllers

import cmwell.ctrl.config.Config
import cmwell.ctrl.utils.ProcUtil

import scala.util.{Failure, Success}

/**
  * Created by michael on 7/12/15.
  */
object CwController extends ComponentController(s"${Config.cmwellHome}/app/ws", "crashableworker", Set("ws")) {
  override def getStartScripts(location: String): Set[String] = Set("cw-start.sh")
}
