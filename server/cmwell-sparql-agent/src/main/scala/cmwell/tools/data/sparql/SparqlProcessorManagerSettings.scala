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
package cmwell.tools.data.sparql

import com.typesafe.config
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class SparqlProcessorManagerSettings {
  val stpSettings: config.Config = ConfigFactory.load()
  val hostConfigFile: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.host-config-file")
  val hostUpdatesSource: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.host-updates-source")
  val useQuadsInSp: Boolean = stpSettings.getBoolean("cmwell.agents.sparql-triggered-processor.use-quads-in-sp")
  val hostWriteOutput: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.host-write-output")
  val materializedViewFormat: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.format")
  val pathAgentConfigs: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.path-agent-configs")
  val writeToken: String = stpSettings.getString("cmwell.agents.sparql-triggered-processor.write-token")
  val initDelay: FiniteDuration =
    stpSettings.getDuration("cmwell.agents.sparql-triggered-processor.init-delay").toMillis.millis
  val maxDelay: FiniteDuration =
    stpSettings.getDuration("cmwell.agents.sparql-triggered-processor.max-delay").toMillis.millis
  val interval: FiniteDuration =
    stpSettings.getDuration("cmwell.agents.sparql-triggered-processor.config-polling-interval").toMillis.millis
  val infotonGroupSize: Integer = stpSettings.getInt("cmwell.triggeredProcessor.infoton-group-size")
  val sensorAlertDelay : java.time.Duration = stpSettings.getDuration("cmwell.agents.sparql-triggered-processor.sensor-alert-delay")

  val irwServiceDaoHostName = stpSettings.getString("irwServiceDao.hostName")
  val irwServiceDaoClusterName = stpSettings.getString("irwServiceDao.clusterName")
  val irwServiceDaoKeySpace2 = stpSettings.getString("irwServiceDao.keySpace2")

}
