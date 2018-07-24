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
import cmwell.zstore.ZStore
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.common.unit.TimeValue
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FlatSpec, Matchers}

import concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source

@DoNotDiscover
class BGSequentialSpec extends FlatSpec with BeforeAndAfterAll with Matchers with LazyLogging {

  var kafkaProducer:KafkaProducer[Array[Byte], Array[Byte]] = _
  var cmwellBGActor:ActorRef = _
  var dao:Dao = _
  var irwService:IRWService = _
  var zStore:ZStore = _
  var offsetsService:OffsetsService = _
  var ftsServiceES:FTSServiceNew = _
  var bgConfig:Config = _
  var actorSystem:ActorSystem = _

  override def beforeAll = {
    val producerProperties = new Properties
    producerProperties.put("bootstrap.servers", "localhost:9092")
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](producerProperties)

    dao = Dao("Test","data2")
    irwService = IRWService.newIRW(dao, 25 , true, 120.seconds)
    // truncate all tables
    Await.ready(irwService.purgeAll(), 20.seconds)

    zStore = ZStore(dao)
    offsetsService = new ZStoreOffsetsService(zStore)
    ftsServiceES = FTSServiceNew("es.test.yml")

    // wait for green status
    ftsServiceES.client.admin().cluster()
      .prepareHealth()
      .setWaitForGreenStatus()
      .setTimeout(TimeValue.timeValueMinutes(5))
      .execute()
      .actionGet()

    // delete all existing indices
    ftsServiceES.client.admin().indices().delete(new DeleteIndexRequest("_all"))

    // load indices template
    val indicesTemplate = Source.fromURL(this.getClass.getResource("/indices_template_new.json")).getLines.reduceLeft(_ + _)
    ftsServiceES.client.admin().indices().preparePutTemplate("indices_template").setSource(indicesTemplate).execute().actionGet()

    bgConfig = ConfigFactory.load

    actorSystem = ActorSystem("cmwell-bg-test-system")

    cmwellBGActor = actorSystem.actorOf(CMWellBGActor.props(0, bgConfig, irwService, ftsServiceES, zStore, offsetsService))

    // scalastyle:off
    println("waiting 10 seconds for all components to load")
    Thread.sleep(10000)
    // scalastyle:on

  }


  "BG" should "process priority commands" in {
    // prepare sequence of priority writeCommands
    val pWriteCommands = Seq.tabulate(2000) { n =>
      val infoton = ObjectInfoton(
        path = s"/cmt/cm/bg-test-priority-before-batch/prio/info$n",
        dc = "dc",
        indexTime = None,
        fields = Some(Map("country" -> Set(FieldValue("Egypt"), FieldValue("Israel")))))
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
        fields = Some(Map("country" -> Set(FieldValue("Egypt"), FieldValue("Israel")))))
      WriteCommand(infoton)
    }

    // make kafka records out of the commands
    val records = writeCommands.map { writeCommand =>
      val commandBytes = CommandSerializer.encode(writeCommand)
      new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
    }

    val f1 = Future{
      records.foreach{ r =>
        kafkaProducer.send(r)
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)
    val f2 = scheduleFuture(1000.millisecond){
      Future{
        pRecords.foreach{ r =>
          kafkaProducer.send(r)
        }
      }(scala.concurrent.ExecutionContext.Implicits.global)
    }

    val assertFut = scheduleFuture(25.seconds) {
      f2.flatMap { _ =>
        logger error s"Finished sending"
        val res = ftsServiceES.search(
          pathFilter = Some(PathFilter("/cmt/cm/bg-test-priority-before-batch/prio", true)),
          fieldsFilter = None,
          datesFilter = None,
          paginationParams = PaginationParams(0, 3000),
          sortParams = SortParam.indexTimeDescending,
          withHistory = false,
          withDeleted = false
        )(scala.concurrent.ExecutionContext.Implicits.global,logger)

        withClue(res){
          res.map{ _.infotons.size should equal(2000)}(scala.concurrent.ExecutionContext.Implicits.global)
        }
      }(scala.concurrent.ExecutionContext.Implicits.global)
    }

    Await.result(assertFut, 30.seconds)
  }

    override def afterAll() = {
      cmwellBGActor ! ShutDown
      Thread.sleep(2000)
      ftsServiceES.shutdown()
      irwService = null
    }
}
