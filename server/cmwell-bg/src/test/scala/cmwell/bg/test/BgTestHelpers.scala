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

package cmwell.bg.test

import java.util.Properties

import cmwell.driver.Dao
import cmwell.fts.FTSService
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.kafka.clients.producer.KafkaProducer
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.create.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.common.xcontent.XContentType
import concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.io.Source

object BgTestHelpers {
  def kafkaProducer(bootstrapServers: String)= {
    val producerProperties = new Properties
    producerProperties.put("bootstrap.servers", bootstrapServers)
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaProducer[Array[Byte], Array[Byte]](producerProperties)
  }

  def dao(address: String, port: Int) = {
    // scalastyle:off
    val initCommands = Some(List(
      "CREATE KEYSPACE IF NOT EXISTS data2 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
      "CREATE TABLE IF NOT EXISTS data2.Path ( path text, uuid text, last_modified timestamp, PRIMARY KEY ( path, last_modified, uuid ) ) WITH CLUSTERING ORDER BY (last_modified DESC, uuid ASC) AND compression = { 'class' : 'LZ4Compressor' } AND caching = {'keys':'ALL', 'rows_per_partition':'1'};",
      "CREATE TABLE IF NOT EXISTS data2.Infoton (uuid text, quad text, field text, value text, data blob, PRIMARY KEY (uuid,quad,field,value)) WITH compression = { 'class' : 'LZ4Compressor' } AND caching = {'keys':'ALL', 'rows_per_partition':'1000'};"
    ))
    // scalastyle:on
    Dao("Test","data2", address, port, initCommands = initCommands)
  }

  def ftsOverridesConfig(address: String, port: Int) = {
    ConfigFactory.load()
      .withValue("ftsService.clusterName", ConfigValueFactory.fromAnyRef("docker-cluster"))
      .withValue("ftsService.transportAddress", ConfigValueFactory.fromIterable(java.util.Arrays.asList(address)))
      .withValue("ftsService.transportPort", ConfigValueFactory.fromAnyRef(port))
  }

  def initFTSService(ftsService: FTSService) = {
    val putTemplateRequest = new PutIndexTemplateRequest("indices_template")
    val indicesTemplateStr = {
      val templateSource = Source.fromURL(this.getClass.getResource("/indices_template.json"))
      try templateSource.getLines.mkString("\n") finally templateSource.close()
    }
    putTemplateRequest.source(indicesTemplateStr, XContentType.JSON)
    val putTemplatePromise = Promise[AcknowledgedResponse]()
    ftsService.client.admin().indices().putTemplate(putTemplateRequest, new ActionListener[AcknowledgedResponse] {
      override def onResponse(response: AcknowledgedResponse): Unit = putTemplatePromise.success(response)
      override def onFailure(e: Exception): Unit = putTemplatePromise.failure(e)
    })
    val putTemplateAck = Await.result(putTemplatePromise.future, 1.minute)
    if (!putTemplateAck.isAcknowledged)
      throw new Exception("ES didn't acknowledge the put template request")
    val createIndexPromise = Promise[AcknowledgedResponse]()
    ftsService.client.admin().indices().create(new CreateIndexRequest("cm_well_p0_0"), new ActionListener[CreateIndexResponse] {
      override def onResponse(response: CreateIndexResponse): Unit = createIndexPromise.success(response)
      override def onFailure(e: Exception): Unit = createIndexPromise.failure(e)
    })
    val createIndexResponse = Await.result(putTemplatePromise.future, 1.minute)
    if (!createIndexResponse.isAcknowledged)
      throw new Exception("ES didn't acknowledge the create index request")
  }

}
