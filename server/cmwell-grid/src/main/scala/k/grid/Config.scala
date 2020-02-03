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

import com.typesafe.config.ConfigFactory

/**
  * Created by michael on 7/1/15.
  */
object Config {

  val config = ConfigFactory.load()

  val clusterName = config.getString("cmwell.grid.clusterName")
  val host = config.getString("cmwell.grid.bind.host")
  val port = config.getInt("cmwell.grid.bind.port")
  val seeds = config.getString("cmwell.grid.seeds").split(",").toSet

  val monitorPort = config.getInt("cmwell.grid.monitor.port")
  val minMembers = config.getInt("cmwell.grid.min-members")
  val labels = config.getString("cmwell.grid.labels").split(",").toSet
  val akkaClusterRole = "GridNode"

  val controllerMemberName = "ctrl"
  val possibleRegFails = config.getInt("cmwell.grid.health.possible-registration-fails")

  val dmapDataDir = config.getString("cmwell.grid.dmap.persistence.data-dir")
}
