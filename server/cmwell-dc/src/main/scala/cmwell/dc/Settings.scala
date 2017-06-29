/**
  * Copyright 2015 Thomson Reuters
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


/*
 * Created by michael on 6/23/15.
 */
object Settings {

  val config = ConfigFactory.load()

  val clusterName = config.getString("cmwell.clusterName")
  val hostName = config.getString("cmwell.grid.bind.host")
  val seeds = config.getString("cmwell.grid.seeds").split(",").toSet
  val port = config.getInt("cmwell.grid.bind.port")

  val target = config.getString("cmwell.dc.target").split(",").toVector

  private[this] val hostPort = """([^:/]+)(:\d+)?""".r
  val destinationHostsAndPorts = target.map{
    case hostPort(h,p) => h -> Option(p).map(_.tail.toInt)
  }

  val minInfotonsPerSync = config.getInt("cmwell.dc.infotonsPerSync.min")
  val maxInfotonsPerSync = config.getInt("cmwell.dc.infotonsPerSync.max")
  require(minInfotonsPerSync < maxInfotonsPerSync)

  val minInfotonsPerBufferingSync = config.getInt("cmwell.dc.infotonsPerBufferingSync.min")
  val maxInfotonsPerBufferingSync = config.getInt("cmwell.dc.infotonsPerBufferingSync.max")
  require(minInfotonsPerBufferingSync < maxInfotonsPerBufferingSync)

  val maxStatementLength = {
    val l = config.getBytes("cmwell.dc.maxStatementLength")
    require(l < Int.MaxValue)
    l.toInt
  }
  val indexTimeThreshold = config.getLong("cmwell.dc.indexTimeThreshold") //in milliseconds
  val maxRemotes = config.getInt("cmwell.dc.maxRemotes")
  val maxBuffers = config.getInt("cmwell.dc.maxBuffers")
  val _outBulkSize = config.getInt("cmwell.dc.bulkSize")
  val _owBulkSize = config.getInt("cmwell.dc.bulkPushSize")
  val altSyncMethod = config.getBoolean("cmwell.dc.altSyncMethod")

  val streamOp = config.getString("cmwell.dc.stream.op")

  val uuidsCacheSize = config.getInt("cmwell.dc.cacheSize") //soft requirement. may be more
  val overlap = config.getLong("cmwell.dc.overlap") //in milliseconds
  val maxRetrieveInfotonCount = config.getInt("cmwell.dc.pull.maxRetrieveInfotonCount")
  val maxRetrieveByteSize = config.getBytes("cmwell.dc.pull.maxRetrieveByteSize")
  val maxTotalInfotonCountAggregatedForRetrieve = config.getInt("cmwell.dc.pull.maxTotalInfotonCountAggregatedForRetrieve")
//  val tsvBufferSize = config.getInt("cmwell.dc.pull.tsvBufferSize")
  val initialTsvRetryCount = config.getInt("cmwell.dc.pull.initialTsvRetryCount")
  val consumeFallbackDuration = config.getDuration("cmwell.dc.pull.consumeFallbackDuration").toMillis
//  val delayInSecondsBetweenNoContentRetries = config.getInt("cmwell.dc.pull.delayInSecondsBetweenNoContentRetries")
  val retrieveParallelism = config.getInt("cmwell.dc.pull.retrieveParallelism")
  val initialBulkRetrieveRetryCount = config.getInt("cmwell.dc.pull.initialBulkRetrieveRetryCount")
  val maxBulkRetrieveRetryDelay = config.getDuration("cmwell.dc.pull.maxBulkRetrieveRetryDelay").toMillis millis
  val initialSingleRetrieveRetryCount = config.getInt("cmwell.dc.pull.initialSingleRetrieveRetryCount")
  val maxSingleRetrieveRetryDelay = config.getDuration("cmwell.dc.pull.maxSingleRetrieveRetryDelay").toMillis millis
  val retrieveRetryQueueSize = config.getInt("cmwell.dc.pull.retrieveRetryQueueSize")
  val maxIngestInfotonCount = config.getInt("cmwell.dc.push.maxIngestInfotonCount")
  val maxIngestByteSize = config.getBytes("cmwell.dc.push.maxIngestByteSize")
  val maxTotalInfotonCountAggregatedForIngest = config.getInt("cmwell.dc.push.maxTotalInfotonCountAggregatedForIngest")
  val initialBulkIngestRetryCount = config.getInt("cmwell.dc.push.initialBulkIngestRetryCount")
  val initialSingleIngestRetryCount = config.getInt("cmwell.dc.push.initialSingleIngestRetryCount")
  val ingestRetryDelay = config.getDuration("cmwell.dc.push.ingestRetryDelay").toMillis millis
  val ingestServiceUnavailableDelay = config.getDuration("cmwell.dc.push.ingestServiceUnavailableDelay").toMillis millis
  val ingestParallelism = config.getInt("cmwell.dc.push.ingestParallelism")
  val ingestRetryQueueSize = config.getInt("cmwell.dc.push.ingestRetryQueueSize")

  val dcaUserToken = config.getString("dcaUser.token")

  val maxRetries = config.getInt("cmwell.dc.retries.max")
  val maxPullRequests = config.getInt("cmwell.dc.requests.maxPull")
  val maxPushRequests = config.getInt("cmwell.dc.requests.maxPush")

  lazy val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
  lazy val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
  lazy val irwServiceDaoKeySpace2 = config.getString("irwServiceDao.keySpace2")
}
