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

import domain.testUtil.InfotonGenerator.genericSystemFields
import java.util.concurrent.Executors

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import cmwell.bg.{CMWellBGActor, ShutDown}
import cmwell.common.{CommandSerializer, OffsetsService, WriteCommand, ZStoreOffsetsService}
import cmwell.domain.{FieldValue, ObjectInfoton}
import cmwell.driver.Dao
import cmwell.fts._
import cmwell.irw.IRWService
import cmwell.util.concurrent.SimpleScheduler.scheduleFuture
import cmwell.zstore.ZStore
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class BGSequentialSpec extends FlatSpec with BeforeAndAfterAll with BgEsCasKafkaZookeeperDockerSuite with Matchers with LazyLogging {

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
    kafkaProducer = BgTestHelpers.kafkaProducer(s"${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)}")
    dao = BgTestHelpers.dao(cassandraContainer.containerIpAddress, cassandraContainer.mappedPort(9042))
    irwService = IRWService.newIRW(dao, 25 , true, 120.seconds)
    // truncate all tables - not needed anymore since the docker images are started fresh each time
    //Await.ready(irwService.purgeAll(), 20.seconds)
    zStore = ZStore(dao)
    offsetsService = new ZStoreOffsetsService(zStore)
    val ftsOverridesConfig = BgTestHelpers.ftsOverridesConfig(elasticsearchContainer.containerIpAddress, elasticsearchContainer.mappedPort(9300))
    ftsServiceES = FTSService(ftsOverridesConfig)
    BgTestHelpers.initFTSService(ftsServiceES)
    bgConfig = ftsOverridesConfig
      .withValue("cmwell.bg.kafka.bootstrap.servers", ConfigValueFactory.fromAnyRef(s"${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)}"))
    actorSystem = ActorSystem("cmwell-bg-test-system")
    cmwellBGActor = actorSystem.actorOf(CMWellBGActor.props(0, bgConfig, irwService, ftsServiceES, zStore, offsetsService))
  }

  "BG" should "process priority commands" in {
    implicit val ex = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))

    // prepare sequence of priority writeCommands
    val pWriteCommands = Seq.tabulate(2000) { n =>
      val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test-priority-before-batch/prio/info$n", lastModifiedBy = "Baruch"),
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
      val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test-priority-before-batch/batch/info$n", lastModifiedBy = "Baruch"),
        fields = Some(Map("country" -> Set(FieldValue("Egypt"), FieldValue("Israel")))))
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
    }
    val f2 = scheduleFuture(1000.millisecond) {
      Future {
        pRecords.foreach { r =>
          kafkaProducer.send(r)
        }
      }
    }

    val assertFut = f2.flatMap { _ =>
      cmwell.util.concurrent.spinCheck(250.millis, true, 60.seconds) {
        ftsServiceES.search(
          pathFilter = Some(PathFilter("/cmt/cm/bg-test-priority-before-batch/prio", true)),
          fieldsFilter = Some(FieldFilter(Must, Equals, "system.lastModifiedBy", "Baruch")),
          datesFilter = None,
          paginationParams = PaginationParams(0, 3000),
          sortParams = SortParam("system.indexTime" -> Desc),
          withHistory = false,
          withDeleted = false
        )
      }(_.infotons.size == 2000)
        .map { res =>
          withClue(res) {
            res.infotons.size should equal(2000)
          }
        }
    }
    Await.result(assertFut, 50.seconds)
  }
    override def afterAll() = {
      val timeout = 30.seconds
      val future = (cmwellBGActor ? ShutDown)(Timeout(timeout))
      val result = Await.result(future, timeout)
      ftsServiceES.shutdown()
      dao.shutdown()
      kafkaProducer.close()
    }
}



