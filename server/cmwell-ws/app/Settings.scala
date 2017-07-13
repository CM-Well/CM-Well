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


package cmwell.ws

import java.time.temporal.TemporalUnit

import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.ConfigFactory
import concurrent.duration._
import scala.util._

/**
 * Created with IntelliJ IDEA.
 * User: Israel
 * Date: 12/09/13
 * Time: 16:08
 * To change this template use File | Settings | File Templates.
 */
object Settings {
  val hostName = java.net.InetAddress.getLocalHost.getHostName

  val config = ConfigFactory.load()

  // Feature Flags
  lazy val newBGFlag = config.getBoolean("cmwell.flag.newBG")
  lazy val oldBGFlag = config.getBoolean("cmwell.flag.oldBG")

  // tLogs DAO
  lazy val tLogsDaoHostName = config.getString("tLogs.hostName")
  lazy val tLogsDaoClusterName = config.getString("tLogs.cluster.name")
  lazy val tLogsDaoKeySpace = config.getString("tLogs.keyspace")
  lazy val tLogsDaoColumnFamily = config.getString("tLogs.columnFamilyName")
  lazy val tLogsDaoMaxConnections = config.getInt("tLogs.maxConnections")

  // kafka
  lazy val kafkaURL = config.getString("kafka.url")
  lazy val persistTopicName = config.getString("kafka.persist.topic.name")
  lazy val zkServers = config.getString("kafka.zkServers")

  // updates tLog
  lazy val updatesTLogName = config.getString("updatesTlog.name")
  lazy val updatesTLogPartition = try { config.getString("updatesTlog.partition") } catch { case _: Throwable => "updatesPar_" + hostName }

  // uuids tLog
  lazy val uuidsTLogName = config.getString("uuidsTlog.name")
  lazy val uuidsTLogPartition = try { config.getString("uuidsTlog.partition") } catch { case _: Throwable => "uuidsPar_" + hostName }

  // infotons DAO
  lazy val irwServiceDaoHostName = config.getString("irwServiceDao.hostName")
  lazy val irwServiceDaoClusterName = config.getString("irwServiceDao.clusterName")
  lazy val irwServiceDaoKeySpace = config.getString("irwServiceDao.keySpace")
  lazy val irwServiceDaoKeySpace2 = config.getString("irwServiceDao.keySpace2")

  lazy val irwReadCacheEnabled = config.getBoolean("webservice.irwServiceDao.readCache.enabled")

  lazy val pollingInterval = config.getLong("indexer.pollingInterval")
  lazy val bucketsSize = config.getInt("indexer.bucketsSize")

  // size is in MB
  lazy val maxUploadSize = config.getInt("webservice.max.upload.size")
  // size is number of infotons
  lazy val maxBulkSize = config.getInt("webservice.max.bulkCommand.size")
  lazy val maxBulkWeight = config.getBytes("webservice.max.bulkCommand.weight")
  //maximum weight of a single field value
  lazy val maxValueWeight: Long = Try(config.getBytes("webservice.max.value.weight")) match {
    case Success(n) => n
    case Failure(_) => 16384L
  }
  lazy val cassandraBulkSize: Int = config.getInt("cassandra.bulk.size")
  lazy val consumeBulkThreshold: Long = config.getLong("cmwell.ws.consume.bulk.threshold")
  lazy val consumeBulkBinarySearchTimeout: FiniteDuration = config.getDuration("cmwell.ws.consume.bulk.binarySearchTimeout").toMillis.millis
  lazy val elasticsearchScrollBufferSize: Int = config.getInt("elasticsearch.scroll.buffer.size")

  //in seconds:
  lazy val cacheTimeout: Long = Try(config.getDuration("webservice.cache.timeout", java.util.concurrent.TimeUnit.SECONDS)).getOrElse(7L)

  lazy val fieldsNamesCacheTimeout: Duration = Try(config.getDuration("cmwell.ws.cache.fieldsNamesTimeout")).toOption.fold(2.minutes) { d =>
    Duration.fromNanos(d.toNanos)
  }


  lazy val pushbackpressure: String = Try(config.getString("cmwell.ws.pushbackpressure.trigger")).getOrElse("old")
  lazy val nbgToggler: Boolean = Try(config.getBoolean("cmwell.ws.nbg")).getOrElse(false)
  lazy val maximumQueueBuildupAllowedUTLog: Long = Try(config.getLong("cmwell.ws.tlog.updating.limit")).toOption.getOrElse(13200000L)
  lazy val maximumQueueBuildupAllowedITLog: Long = Try(config.getLong("cmwell.ws.tlog.indexing.limit")).toOption.getOrElse(3500000L)
  lazy val maximumQueueBuildupAllowed: Long = Try(config.getLong("cmwell.ws.klog.limit")).toOption.getOrElse(496351L)
  lazy val ingestPushbackByServer: FiniteDuration = Try(config.getDuration("cmwell.ws.klog.pushback.time")).toOption.fold(7.seconds) { d =>
    Duration.fromNanos(d.toNanos)
  }
  lazy val bgMonitorAskTimeout: FiniteDuration = Try(config.getDuration("cmwell.ws.klog.pushback.timeout")).toOption.fold(5.seconds) { d =>
    Duration.fromNanos(d.toNanos)
  }

  // default timeout for ElasticSearch calls
  lazy val esTimeout = config.getInt("ftsService.default.timeout").seconds

  lazy val gridBindIP = config.getString("cmwell.grid.bindIP")
  lazy val gridBindPort = config.getInt("cmwell.grid.bindPort")
  lazy val gridSeeds = Set.empty[String] ++ config.getString("cmwell.grid.seeds").split(";")
  lazy val clusterName = config.getString("cmwell.clusterName")

  lazy val authSystemVersion = config.getInt("auth.system.version")
  lazy val maxSearchContexts = config.getLong("webservice.max.search.contexts")
  lazy val expansionLimit = config.getInt("webservice.xg.limit")
  lazy val chunkSize = config.getBytes("webservice.max.chunk.size")
  lazy val maxOffset = config.getInt("webservice.max-offset")
  lazy val maxLength = Try(config.getInt("webservice.max-length")).getOrElse(expansionLimit)

  lazy val maxQueryResultsLength = config.getInt("crashableworker.results.maxLength")
  lazy val queryResultsTempFileBaseName = config.getString("crashableworker.results.baseFileName")

  lazy val dataCenter = config.getString("dataCenter.id")
  lazy val maxDataCenters = config.getInt("dataCenter.maxInstances")

  lazy val quadsCacheSize = config.getLong("quads.cache.size")

  lazy val xFixNumRetries = config.getInt("xfix.num.retries")

  lazy val maxSearchResultsForGlobalQuadOperations = config.getInt("quads.globalOperations.results.maxLength")
  lazy val initialMetaNsLoadingAmount = config.getInt("ws.meta.ns.initialLoadingAmount")

  lazy val esGracfulDegradationTimeout = config.getInt("ws.es.gracfulDegradationTimeout")

  lazy val graphReplaceSearchTimeout = config.getInt("webservice.graphreplace.search.timeoutsec")
  lazy val graphReplaceMaxStatements = config.getInt("webservice.graphreplace.maxStatements")

  lazy val maxDaysToAllowGenerateTokenFor = config.getInt("authorization.token.expiry.maxDays")

  lazy val loginPenalty = config.getInt("webservice.login.penaltysec")

  lazy val requestsPenaltyThreshold = config.getInt("cmwell.ws.trafficshaping.requests-penalty-threshold")
  lazy val checkFrequency = config.getInt("cmwell.ws.trafficshaping.check-frequency-sec")
  lazy val defaultLimitForHistoryVersions = config.getInt("cmwell.ws.cassandra-driver.history-versions-limit")
  lazy val startWithNewBackendEnabled = config.getBoolean("cmwell.ws.nbg")
  lazy val maxRequestTimeSec = config.getInt("cmwell.ws.trafficshaping.max-request-time-sec")
  lazy val stressThreshold = config.getLong("cmwell.ws.trafficshaping.stress-threshold")
  lazy val thresholdToUseZStore = config.getBytes("cmwell.ws.switch-over-to-zstore.file-size")

  lazy val zCacheSecondsTTL = config.getInt("cmwell.ws.zcache.ttlSeconds")
  lazy val zCachePollingMaxRetries = config.getInt("cmwell.ws.zcache.pollingMaxRetries")
  lazy val zCachePollingIntervalSeconds = config.getInt("cmwell.ws.zcache.pollingIntervalSeconds")
  lazy val zCacheL1Size = config.getInt("cmwell.ws.zcache.L1Size")
}
