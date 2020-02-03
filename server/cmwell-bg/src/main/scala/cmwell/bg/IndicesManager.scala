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
/*
package cmwell.bg

import akka.actor.{Actor, Props}
import cmwell.fts.FTSService
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.action.admin.indices.alias.Alias
import collection.JavaConverters._
import concurrent.duration._

/**
  * Created by israel on 25/07/2016.
  */
object IndicesManager {
  def props(ftsService: FTSService, config: Config) = Props(new IndicesManager(ftsService, config))
}

class IndicesManager(ftsService: FTSService, config: Config) extends Actor with LazyLogging {

  import context._

  val allIndicesAliasName = config.getString("cmwell.bg.allIndicesAliasName")
  val latestIndexAliasName = config.getString("cmwell.bg.latestIndexAliasName")
  val indexNamePrefix = config.getString("cmwell.bg.indexNamePrefix")
  val maxDocsPerShard = config.getLong("cmwell.bg.maxDocsPerShard")
  //TODO return the line below after solving JVM arg with space problem
//  val maintainIndicesInterval = Duration.fromNanos(config.getDuration("cmwell.bg.maintainIndicesInterval").toNanos)
  val maintainIndicesInterval = Duration.apply(config.getLong("cmwell.bg.maintainIndicesInterval"), "minutes")

  logger.info(s"""config params:
                 |allIndicesAliasName:$allIndicesAliasName
                 |latestIndexAliasName:$latestIndexAliasName
                 |indexNamePrefix:$indexNamePrefix
                 |maxDocsPerShard:$maxDocsPerShard
                 |maintainIndicesInterval:$maintainIndicesInterval""".stripMargin)

  override def preStart(): Unit = self ! CheckIndices

  override def receive: Receive = {
    case CheckIndices =>
      maintainIndices()

    case UpdateCurrentAlias(previousIndex, newIndex) =>
      val replaceCurrentIndexAliasRes = ftsService.client
        .admin()
        .indices()
        .prepareAliases()
        .removeAlias(previousIndex, latestIndexAliasName)
        .addAlias(newIndex, latestIndexAliasName)
        .execute()
        .actionGet()
      logger.info(
        s"Replace current index alias from: $previousIndex to: $newIndex acknowledged: ${replaceCurrentIndexAliasRes.isAcknowledged}"
      )
      context.system.scheduler.scheduleOnce(maintainIndicesInterval, self, CheckIndices)

  }

  private def maintainIndices() = {
    logger.debug("maintaining indices")
    val indicesStats =
      ftsService.client.admin().indices().prepareStats(allIndicesAliasName).clear().setDocs(true).execute().actionGet()

    // find latest index name
    val currentAliasRes =
      ftsService.client.admin.indices().prepareGetAliases(latestIndexAliasName).execute().actionGet()
    val lastCurrentIndexName = currentAliasRes.getAliases.keysIt().next()
    val numOfDocumentsCurrent = indicesStats.getIndex(lastCurrentIndexName).getTotal.getDocs.getCount
    val lastCurrentIndexRecovery = ftsService.client.admin().indices().prepareRecoveries(latestIndexAliasName).get()
    val numOfShardsCurrent = lastCurrentIndexRecovery
      .shardResponses()
      .get(lastCurrentIndexName)
      .asScala
      .filter(_.recoveryState().getPrimary)
      .size

    logger.debug(s"number of docs per shard:${numOfDocumentsCurrent / numOfShardsCurrent}")

    // If number of document per shard in latest index is greater than threshold
    // create new index while adding it to the appropriate indices
    if ((numOfDocumentsCurrent / numOfShardsCurrent) > maxDocsPerShard) {
      logger.info(
        s"number of docs per shard:${numOfDocumentsCurrent / numOfShardsCurrent} has passed the threshold of: $maxDocsPerShard , shifting gear"
      )
      // create new index
      val lastCurrentIndexCounter =
        lastCurrentIndexName.substring(lastCurrentIndexName.lastIndexOf('_') + 1, lastCurrentIndexName.length).toInt
      val nextCurrentIndexName = indexNamePrefix + (lastCurrentIndexCounter + 1)

      // create new index while adding it to 'latest' alias
      val createNextCurrentIndexRes = ftsService.client
        .admin()
        .indices()
        .prepareCreate(nextCurrentIndexName)
        .addAlias(new Alias(allIndicesAliasName))
        .execute()
        .actionGet()

      logger.info(
        s"Create new current index named:$nextCurrentIndexName acknowledged: ${createNextCurrentIndexRes.isAcknowledged}"
      )

      system.scheduler.scheduleOnce(10.seconds, self, UpdateCurrentAlias(lastCurrentIndexName, nextCurrentIndexName))

    } else {
      context.system.scheduler.scheduleOnce(maintainIndicesInterval, self, CheckIndices)
      logger.debug("nothing to do this time")
    }

  }

  case object CheckIndices
  case class UpdateCurrentAlias(previousIndex: String, newIndex: String)
}
*/
