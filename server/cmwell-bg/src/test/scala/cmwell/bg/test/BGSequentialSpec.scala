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
import cmwell.util.concurrent.SimpleScheduler.scheduleFuture
import cmwell.util.testSuitHelpers.test.EsCasKafkaZookeeperDockerSuite
import cmwell.zstore.ZStore
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.create.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FlatSpec, Matchers}

import concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.io.Source

class BGSequentialSpec extends FlatSpec with BeforeAndAfterAll with EsCasKafkaZookeeperDockerSuite with Matchers with LazyLogging {
  override def elasticsearchVersion: String = cmwell.util.build.BuildInfo.elasticsearchVersion
  override def cassandraVersion: String = cmwell.util.build.BuildInfo.cassandraVersion
  override def kafkaVersion: String = s"${cmwell.util.build.BuildInfo.scalaVersion.take(4)}-${cmwell.util.build.BuildInfo.kafkaVersion}"
  override def zookeeperVersion: String = cmwell.util.build.BuildInfo.zookeeperVersion


  var kafkaProducer:KafkaProducer[Array[Byte], Array[Byte]] = _
  var cmwellBGActor:ActorRef = _
  var dao:Dao = _
  var irwService:IRWService = _
  var zStore:ZStore = _
  var offsetsService:OffsetsService = _
  var ftsServiceES:FTSService = _
  var bgConfig:Config = _
  var actorSystem:ActorSystem = _

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
    irwService = IRWService.newIRW(dao, 25 , true, 120.seconds)
    // truncate all tables
    Await.ready(irwService.purgeAll(), 20.seconds)

    zStore = ZStore(dao)
    offsetsService = new ZStoreOffsetsService(zStore)
    val ftsOverridesConfig = ConfigFactory.load()
      .withValue("ftsService.clusterName", ConfigValueFactory.fromAnyRef("docker-cluster"))
      .withValue("ftsService.transportAddress", ConfigValueFactory.fromAnyRef(elasticsearchContainer.containerIpAddress))
      .withValue("ftsService.transportPort", ConfigValueFactory.fromAnyRef(elasticsearchContainer.mappedPort(9300)))
    ftsServiceES = FTSService(ftsOverridesConfig)

    val putTemplateRequest = new PutIndexTemplateRequest("indices_template")
    val indicesTemplateStr = {
      val templateSource = Source.fromURL(this.getClass.getResource("/indices_template.json"))
      try templateSource.getLines.mkString("\n") finally templateSource.close()
    }
    putTemplateRequest.source(indicesTemplateStr, XContentType.JSON)
    val putTemplatePromise = Promise[AcknowledgedResponse]()
    ftsServiceES.client.admin().indices().putTemplate(putTemplateRequest, new ActionListener[AcknowledgedResponse] {
      override def onResponse(response: AcknowledgedResponse): Unit = putTemplatePromise.success(response)
      override def onFailure(e: Exception): Unit = putTemplatePromise.failure(e)
    })
    val putTemplateAck = Await.result(putTemplatePromise.future, 1.minute)
    if (!putTemplateAck.isAcknowledged)
      throw new Exception("ES didn't acknowledge the put template request")
    val createIndexPromise = Promise[AcknowledgedResponse]()
    ftsServiceES.client.admin().indices().create(new CreateIndexRequest("cm_well_p0_0"), new ActionListener[CreateIndexResponse] {
      override def onResponse(response: CreateIndexResponse): Unit = createIndexPromise.success(response)
      override def onFailure(e: Exception): Unit = createIndexPromise.failure(e)
    })
    val createIndexResponse = Await.result(putTemplatePromise.future, 1.minute)
    if (!createIndexResponse.isAcknowledged)
      throw new Exception("ES didn't acknowledge the create index request")
    bgConfig = ConfigFactory.load
      .withValue("cmwell.bg.kafka.bootstrap.servers", ConfigValueFactory.fromAnyRef(s"${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)}"))

    actorSystem = ActorSystem("cmwell-bg-test-system")

    cmwellBGActor = actorSystem.actorOf(CMWellBGActor.props(0, bgConfig, irwService, ftsServiceES, zStore, offsetsService))
  }


  "BG" should "process priority commands" in {
    // prepare sequence of priority writeCommands
    val pWriteCommands = Seq.tabulate(2000) { n =>
      val infoton = ObjectInfoton(
        path = s"/cmt/cm/bg-test-priority-before-batch/prio/info$n",
        dc = "dc",
        indexTime = None,
        fields = Some(Map("country" -> Set(FieldValue("Egypt"), FieldValue("Israel")))), protocol = None)
      WriteCommand(infoton)
    }

    // make kafka records out of the commands
    val pRecords = pWriteCommands.map { writeCommand =>
      val commandBytes = CommandSerializer.encode(writeCommand)
      new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic.priority", commandBytes)
    }

    // prepare sequence of priority writeCommands
    val writeCommands = Seq.tabulate(15000) { n =>
      val infoton = ObjectInfoton(
        path = s"/cmt/cm/bg-test-priority-before-batch/batch/info$n",
        dc = "dc",
        indexTime = None,
        fields = Some(Map("country" -> Set(FieldValue("Egypt"), FieldValue("Israel")))), protocol = None)
      WriteCommand(infoton)
    }

    // make kafka records out of the commands
    val records = writeCommands.map { writeCommand =>
      val commandBytes = CommandSerializer.encode(writeCommand)
      new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
    }

    val f1 = Future {
      records.foreach { r =>
        kafkaProducer.send(r)
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)
    val f2 = scheduleFuture(1000.millisecond) {
      Future {
        pRecords.foreach { r =>
          kafkaProducer.send(r)
        }
      }(scala.concurrent.ExecutionContext.Implicits.global)
    }

    val assertFut = f2.flatMap { _ =>
      cmwell.util.concurrent.spinCheck(250.millis, true, 60.seconds) {
        ftsServiceES.search(
          pathFilter = Some(PathFilter("/cmt/cm/bg-test-priority-before-batch/prio", true)),
          fieldsFilter = None,
          datesFilter = None,
          paginationParams = PaginationParams(0, 3000),
          sortParams = SortParam("system.indexTime" -> Desc),
          withHistory = false,
          withDeleted = false
        )(scala.concurrent.ExecutionContext.Implicits.global, logger)
      }(_.infotons.size == 2000)
        .map { res =>
          withClue(res) {
            res.infotons.size should equal(2000)
          }
        }(scala.concurrent.ExecutionContext.Implicits.global)
    }(scala.concurrent.ExecutionContext.Implicits.global)
    Await.result(assertFut, 50.seconds)
  }

    override def afterAll() = {
      cmwellBGActor ! ShutDown
      ftsServiceES.shutdown()
      irwService = null
    }
}
