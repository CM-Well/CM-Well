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
package k.grid.webapi.jsobj

import k.grid.MemoryInfo
import play.api.libs.json._

/**
  * Created by michael on 4/14/16.
  */
case class GridMemoryInfo(clusterName: String, sampleTime: Long, machinesUsages: Set[MachineMemoryInfo]) {
  val toJson: JsObject = {
    Json.obj("clusterName" -> JsString(clusterName),
             "sampleTime" -> JsNumber(sampleTime),
             "machinesUsages" -> JsArray(machinesUsages.toVector.map(_.toJson)))
  }
}
case class MachineMemoryInfo(machineName: String, jvmName: String, mempools: Map[String, Long]) {
  val toJson: JsObject = {
    Json.obj("machineName" -> JsString(machineName),
             "jvmName" -> JsString(jvmName),
             "memoryPools" -> JsArray(mempools.map(mp => Json.obj(mp._1 -> JsNumber(mp._2))).toVector))
  }
}
