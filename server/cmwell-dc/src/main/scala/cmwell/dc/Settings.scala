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
package cmwell.dc

import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

object Settings {
  val config = ConfigFactory.load()
  val clusterName = config.getString("cmwell.clusterName")
  val hostName = config.getString("cmwell.grid.bind.host")
  val seeds = config.getString("cmwell.grid.seeds").split(",").toSet
  val port = config.getInt("cmwell.grid.bind.port")
  val rawTarget = config.getString("cmwell.dc.target")
  private[this] val hostPort = """([^:/]+)(:\d+)?""".r
  def destinationHostsAndPorts(target: String) = target.split(",").toVector.map {
    case hostPort(h, p) => h -> Option(p).map(_.tail.toInt)
  }
  val maxStatementLength = {
    val l = config.getBytes("cmwell.dc.maxStatementLength")
    require(l < Int.MaxValue)
    l.toInt
  }
  val maxRetrieveInfotonCount = config.getInt("cmwell.dc.pull.maxRetrieveInfotonCount")
  val maxRetrieveByteSize = config.getBytes("cmwell.dc.pull.maxRetrieveByteSize")
  val maxTotalInfotonCountAggregatedForRetrieve =
    config.getInt("cmwell.dc.pull.maxTotalInfotonCountAggregatedForRetrieve")
  val tsvBufferSize = config.getInt("cmwell.dc.pull.tsvBufferSize")
  val initialTsvRetryCount = config.getInt("cmwell.dc.pull.initialTsvRetryCount")
  val lockedOnConsume = config.getBoolean("cmwell.dc.pull.lockedOnConsume")
  val bulkTsvRetryCount = config.getInt("cmwell.dc.pull.bulkTsvRetryCount")
  val consumeFallbackDuration = config.getDuration("cmwell.dc.pull.consumeFallbackDuration").toMillis
//  val delayInSecondsBetweenNoContentRetries = config.getInt("cmwell.dc.pull.delayInSecondsBetweenNoContentRetries")
  val retrieveParallelism = config.getInt("cmwell.dc.pull.retrieveParallelism")
  val initialBulkRetrieveRetryCount = config.getInt("cmwell.dc.pull.initialBulkRetrieveRetryCount")
  val maxBulkRetrieveRetryDelay = config.getDuration("cmwell.dc.pull.maxBulkRetrieveRetryDelay").toMillis.millis
  val initialSingleRetrieveRetryCount = config.getInt("cmwell.dc.pull.initialSingleRetrieveRetryCount")
  val maxSingleRetrieveRetryDelay = config.getDuration("cmwell.dc.pull.maxSingleRetrieveRetryDelay").toMillis.millis
  val retrieveRetryQueueSize = config.getInt("cmwell.dc.pull.retrieveRetryQueueSize")
  val maxIngestInfotonCount = config.getInt("cmwell.dc.push.maxIngestInfotonCount")
  val maxIngestByteSize = config.getBytes("cmwell.dc.push.maxIngestByteSize")
  val maxTotalInfotonCountAggregatedForIngest = config.getInt("cmwell.dc.push.maxTotalInfotonCountAggregatedForIngest")
  val initialBulkIngestRetryCount = config.getInt("cmwell.dc.push.initialBulkIngestRetryCount")
  val initialSingleIngestRetryCount = config.getInt("cmwell.dc.push.initialSingleIngestRetryCount")
  val ingestRetryDelay = config.getDuration("cmwell.dc.push.ingestRetryDelay").toMillis.millis
  val ingestServiceUnavailableDelay = config.getDuration("cmwell.dc.push.ingestServiceUnavailableDelay").toMillis.millis
  val gzippedIngest = config.getBoolean("cmwell.dc.push.gzippedIngest")
  val ingestParallelism = config.getInt("cmwell.dc.push.ingestParallelism")
  val ingestRetryQueueSize = config.getInt("cmwell.dc.push.ingestRetryQueueSize")
  val dcaUserToken = config.getString("dcaUser.token")
  val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
  val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
  val irwServiceDaoKeySpace2 = config.getString("irwServiceDao.keySpace2")

}
