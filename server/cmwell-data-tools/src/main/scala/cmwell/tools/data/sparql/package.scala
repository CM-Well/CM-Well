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
package cmwell.tools.data

import cmwell.tools.data.downloader.consumer.Downloader.Token
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats

package object sparql {

  case class AgentTokensAndStatistics(sensors: TokenAndStatisticsMap,
                                      agentIngestStats: Option[IngestStats] = None,
                                      materializedStats: Option[DownloadStats] = None )

  type TokenAndStatisticsMap = Map[String, TokenAndStatistics]
  type TokenAndStatistics = (Token, Option[DownloadStats])

  case class SensorContext(name: String, token: String, horizon: Boolean, remainingInfotons: Option[Long])

  case class Sensor(name: String,
                    qp: String = "",
                    fromIndexTime: Long = 0,
                    path: String,
                    token: Option[String] = None,
                    sparqlToRoot: Option[String] = None) {

    override def toString: String = s"Sensor [name=$name, path=$path, qp=$qp, fromIndexTime=$fromIndexTime]"
  }


}
