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
package cmwell.bg.test

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import cmwell.bg.{CMWellBGActor, ShutDown}
import cmwell.common.{CommandSerializer, OffsetsService, WriteCommand, ZStoreOffsetsService}
import cmwell.domain.{FieldValue, ObjectInfoton}
import cmwell.driver.Dao
import cmwell.fts._
import cmwell.irw.IRWService
import cmwell.util.testSuitHelpers.test.EsCasKafkaZookeeperDockerSuite
import cmwell.zstore.ZStore
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.common.unit.TimeValue
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

/**
  * Created by israel on 13/09/2016.
  */
class BGResilienceSpec extends FlatSpec with BeforeAndAfterAll with EsCasKafkaZookeeperDockerSuite with Matchers with LazyLogging {
  override def elasticsearchVersion: String = cmwell.util.build.BuildInfo.elasticsearchVersion
  override def cassandraVersion: String = cmwell.util.build.BuildInfo.cassandraVersion
  override def kafkaVersion: String = s"${cmwell.util.build.BuildInfo.scalaVersion.take(4)}-${cmwell.util.build.BuildInfo.kafkaVersion}"
  override def zookeeperVersion: String = cmwell.util.build.BuildInfo.zookeeperVersion

  var kafkaProducer:KafkaProducer[Array[Byte], Array[Byte]] = _
  var cmwellBGActor:ActorRef = _
  var dao:Dao = _
  var testIRWMockupService:IRWService = _
  var irwService:IRWService = _
  var zStore:ZStore = _
  var offsetsService:OffsetsService = _
  var ftsServiceES:FTSService = _
  var bgConfig:Config = _
  var actorSystem:ActorSystem = _
  import concurrent.ExecutionContext.Implicits.global

  override def beforeAll = {
    //notify ES to not set Netty's available processors
    System.setProperty("es.set.netty.runtime.available.processors", "false")

    val producerProperties = new Properties
    producerProperties.put("bootstrap.servers", s"${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)}")
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](producerProperties)

    // scalastyle:off
    val initCommands = Some(List(
      "CREATE KEYSPACE IF NOT EXISTS data2 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
      "CREATE TABLE IF NOT EXISTS data2.Path ( path text, uuid text, last_modified timestamp, PRIMARY KEY ( path, last_modified, uuid ) ) WITH CLUSTERING ORDER BY (last_modified DESC, uuid ASC) AND compression = { 'sstable_compression' : 'LZ4Compressor' } AND caching = {'keys':'ALL', 'rows_per_partition':'1'};",
      "CREATE TABLE IF NOT EXISTS data2.Infoton (uuid text, quad text, field text, value text, data blob, PRIMARY KEY (uuid,quad,field,value)) WITH compression = { 'sstable_compression' : 'LZ4Compressor' } AND caching = {'keys':'ALL', 'rows_per_partition':'1000'};"
    ))
    // scalastyle:on
    dao = Dao("Test","data2", cassandraContainer.containerIpAddress, cassandraContainer.mappedPort(9042), initCommands = initCommands)
    testIRWMockupService = FailingIRWServiceMockup(dao, 13)
    zStore = ZStore(dao)
    irwService = IRWService.newIRW(dao, 25 , true, 0.seconds)
    offsetsService = new ZStoreOffsetsService(zStore)

    val ftsOverridesConfig = ConfigFactory.load()
        .withValue("ftsService.clusterName", ConfigValueFactory.fromAnyRef("docker-cluster"))
        .withValue("ftsService.transportAddress", ConfigValueFactory.fromAnyRef(elasticsearchContainer.containerIpAddress))
        .withValue("ftsService.transportPort", ConfigValueFactory.fromAnyRef(elasticsearchContainer.mappedPort(9300)))
    ftsServiceES = FailingFTSServiceMockup(ftsOverridesConfig, 5)

    // delete all existing indices
    ftsServiceES.client.admin().indices().delete(new DeleteIndexRequest("_all"))

    // create current index
    ftsServiceES.client.admin().indices().prepareCreate("cm_well_p0_0").execute().actionGet()

    bgConfig = ftsOverridesConfig
      .withValue("cmwell.bg.esActionsBulkSize", ConfigValueFactory.fromAnyRef(100))
      .withValue("cmwell.bg.kafka.bootstrap.servers", ConfigValueFactory.fromAnyRef(s"${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)}"))

    actorSystem = ActorSystem("cmwell-bg-test-system")

    cmwellBGActor = actorSystem.actorOf(CMWellBGActor.props(0, bgConfig, testIRWMockupService, ftsServiceES, zStore, offsetsService))

  }

  "Resilient BG" should "process commands as usual on circumvented BGActor (periodically failing IRWService) after suspending and resuming" in {

    val numOfCommands = 1500
    // prepare sequence of writeCommands
    val writeCommands = Seq.tabulate(numOfCommands){ n =>
      val infoton = ObjectInfoton(
        path = s"/cmt/cm/bg-test/circumvented_bg/info$n",
        dc = "dc",
        indexTime = None,
        fields = Some(Map("games" -> Set(FieldValue("Taki"), FieldValue("Race")))),
        protocol = None)
      WriteCommand(infoton)
    }

    // make kafka records out of the commands
    val pRecords = writeCommands.map{ writeCommand =>
      val commandBytes = CommandSerializer.encode(writeCommand)
      new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
    }

    // send them all
    pRecords.foreach { kafkaProducer.send(_)}

    // scalastyle:off
    println("waiting for 10 seconds")
    Thread.sleep(10000)
    // scalastyle:on

    for( i <- 0 until numOfCommands) {
      val nextResult = Await.result(irwService.readPathAsync(s"/cmt/cm/bg-test/circumvented_bg/info$i"), 5.seconds)
      withClue(nextResult, s"/cmt/cm/bg-test/circumvented_bg/info$i"){
        nextResult should not be empty
      }
    }

    for( i <- 0 until numOfCommands) {
      val searchResponse = Await.result(
        ftsServiceES.search(
          pathFilter = None,
          fieldsFilter = Some(SingleFieldFilter(Must, Equals, "system.path", Some(s"/cmt/cm/bg-test/circumvented_bg/info$i"))),
          datesFilter = None,
          paginationParams = PaginationParams(0, 200)
        ),
        10.seconds
      )
      withClue(s"/cmt/cm/bg-test/circumvented_bg/info$i"){
        searchResponse.infotons.size should equal(1)
      }
    }
  }

  override def afterAll() = {
    cmwellBGActor ! ShutDown
    ftsServiceES.shutdown()
    testIRWMockupService = null
    irwService = null
  }
}
