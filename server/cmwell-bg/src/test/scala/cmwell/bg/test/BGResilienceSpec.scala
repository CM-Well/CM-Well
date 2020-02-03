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

import java.util.concurrent.Executors
import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import cmwell.bg.{CMWellBGActor, ShutDown}
import cmwell.common.{CommandSerializer, OffsetsService, WriteCommand, ZStoreOffsetsService}
import cmwell.domain.{FieldValue, ObjectInfoton}
import cmwell.driver.Dao
import cmwell.fts._
import cmwell.irw.IRWService
import cmwell.zstore.ZStore
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Inspectors, Matchers}

import scala.concurrent.{Await, ExecutionContext, Future}
import akka.pattern.ask
import domain.testUtil.InfotonGenerator.genericSystemFields

import scala.concurrent.duration._

/**
  * Created by israel on 13/09/2016.
  */
class BGResilienceSpec extends AsyncFlatSpec with BeforeAndAfterAll with BgEsCasKafkaZookeeperDockerSuite with Matchers with LazyLogging with Inspectors {

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

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

  override def beforeAll = {
    //notify ES to not set Netty's available processors
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    kafkaProducer = BgTestHelpers.kafkaProducer(s"${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)}")
    // scalastyle:on
    dao = BgTestHelpers.dao(cassandraContainer.containerIpAddress, cassandraContainer.mappedPort(9042))
    testIRWMockupService = FailingIRWServiceMockup(dao, 13)
    zStore = ZStore(dao)
    irwService = IRWService.newIRW(dao, 25 , true, 0.seconds)
    offsetsService = new ZStoreOffsetsService(zStore)
    val ftsOverridesConfig = BgTestHelpers.ftsOverridesConfig(elasticsearchContainer.containerIpAddress, elasticsearchContainer.mappedPort(9300))
    ftsServiceES = FailingFTSServiceMockup(ftsOverridesConfig, 5)
    // delete all existing indices - not needed the docker is started fresh every time
    //ftsServiceES.client.admin().indices().delete(new DeleteIndexRequest("_all"))
    BgTestHelpers.initFTSService(ftsServiceES)
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
      val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test/circumvented_bg/info$n", dc = "dc", lastModifiedBy = "Baruch"),
        fields = Some(Map("games" -> Set(FieldValue("Taki"), FieldValue("Race")))))
      WriteCommand(infoton)
    }

    // make kafka records out of the commands
    val pRecords = writeCommands.map{ writeCommand =>
      val commandBytes = CommandSerializer.encode(writeCommand)
      new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
    }

    // send them all
    pRecords.foreach {kafkaProducer.send(_)}

    val casCheck = Future.traverse((0 until numOfCommands).toList) {i =>
      cmwell.util.concurrent.spinCheck(1.second, true, 60.seconds) {
        irwService.readPathAsync(s"/cmt/cm/bg-test/circumvented_bg/info$i")} ( _.nonEmpty )
    }

    val esCheck = cmwell.util.concurrent.spinCheck(1.second, true, 120.seconds) {
      try{
        ftsServiceES.search(
          pathFilter = None,
          fieldsFilter = Some(MultiFieldFilter(Must, Seq(
              FieldFilter(Must, Equals, "system.parent.parent_hierarchy", s"/cmt/cm/bg-test/circumvented_bg"),
              FieldFilter(Must, Equals, "system.lastModifiedBy", "Baruch")))),
          datesFilter = None,
          paginationParams = PaginationParams(0, 1)
        )
      } catch {case _ : Throwable => Future.failed(new RuntimeException)}
    } (_.total == 1500)

    for {
      es <- esCheck
      cas <- casCheck
    } yield {
      es.total should equal(numOfCommands)
      forAll(cas) ( _ should not be empty)
    }

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
