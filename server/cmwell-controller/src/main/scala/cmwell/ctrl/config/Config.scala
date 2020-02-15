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

import com.typesafe.config.ConfigFactory

/**
  * Created by michael on 12/2/14.
  */
object Config {
  val hostName = java.net.InetAddress.getLocalHost.getHostName

  val config = ConfigFactory.load()

  val host = config.getString("cmwell.grid.bind.host")
  val seedPort = config.getInt("ctrl.seedPort")
  val listenAddress = config.getString("ctrl.externalHostName")
  val seedNodes = config.getString("ctrl.seedNodes").split(",").toSet
  val clusterName = config.getString("ctrl.clusterName")
  val roles = config.getString("ctrl.roles").split(",").toSet

  val sprayPort = config.getInt("ctrl.sprayPort")

  val commandActorName = config.getString("ctrl.commandActorName")
  val serverRole = config.getString("ctrl.serverRole")
  val clientRole = config.getString("ctrl.clientRole")

  val webAddress = config.getString("ctrl.externalHostName")
  val webPort = config.getInt("ctrl.webPort")
  val pingIp = config.getString("ctrl.pingIp")

  val historySize = config.getInt("ctrl.hc.historySize")
  val singletonStarter = config.getBoolean("ctrl.singletonStarter")

  val gcSampleInterval = config.getLong("ctrl.agent.gcSampleInterval")

  val cmwellHome = config.getString("ctrl.home")
  val javaBinPath = s"$cmwellHome/app/java/bin"

  val esMasters = config.getInt("ctrl.hc.esMasters")
  val idleWaitSeconds = config.getInt("ctrl.hc.idleWaitSeconds")
  val hcSampleInterval = config.getInt("ctrl.hc.hcSampleInterval")
  val bgSampleInterval = config.getInt("ctrl.hs.bgSampleInterval")
  val downNodesGraceTimeMinutes = config.getInt("ctrl.hc.downNodesGraceTimeMinutes")
  val isolationCheckSeconds = config.getInt("ctrl.hc.isolationCheckSeconds")

  val heakupSampleInterval = config.getInt("ctrl.agent.heakupSampleInterval")
  val heakupReportInterval = config.getInt("ctrl.agent.heakupReportInterval")

  val webFailCount = config.getInt("ctrl.hc.webFailCount")
}
