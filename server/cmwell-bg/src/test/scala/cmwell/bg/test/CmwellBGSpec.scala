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
import java.nio.file.{Files, Paths}

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import cmwell.bg.{CMWellBGActor, ShutDown}
import cmwell.common._
import cmwell.domain._
import cmwell.driver.Dao
import cmwell.fts._
import cmwell.irw.IRWService
import cmwell.util.FullBox
import cmwell.util.concurrent.SimpleScheduler.{schedule, scheduleFuture}
import cmwell.zstore.ZStore
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.joda.time.DateTime
import org.scalatest.OptionValues._
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, _}
import scala.util.Random


/**
  * Created by israel on 15/02/2016.
  */
class CmwellBGSpec extends AsyncFunSpec with BeforeAndAfterAll with BgEsCasKafkaZookeeperDockerSuite with Matchers with Inspectors with LazyLogging {

  var kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]] = _
  var cmwellBGActor: ActorRef = _
  var dao: Dao = _
  var irwService: IRWService = _
  var zStore:ZStore = _
  var offsetsService:OffsetsService = _
  var ftsServiceES: FTSService = _
  var bgConfig: Config = _
  var actorSystem: ActorSystem = _
  val okToStartPromise = Promise[Unit]()


  def sendToKafkaProducer(pRecord: ProducerRecord[Array[Byte], Array[Byte]]): Future[RecordMetadata] = {
    val p = Promise[RecordMetadata]()
    kafkaProducer.send(pRecord, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if(metadata ne null) p.success(metadata)
        else p.failure(exception)
      }
    })
    p.future
  }

  def executeAfterCompletion[T](f: Future[_], timeout: FiniteDuration = 5.minutes)(body: =>Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]()
    f.onComplete(_ => p.tryCompleteWith(body))(ec)
    if(timeout != Duration.Zero) {
      schedule(timeout)(p.tryFailure(new Exception("timeout")))(ec)
    }
    p.future
  }

  override def beforeAll = {
    //notify ES to not set Netty's available processors
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    kafkaProducer = BgTestHelpers.kafkaProducer(s"${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)}")
    Files.deleteIfExists(Paths.get("./target", "persist_topic-0.offset"))
    Files.deleteIfExists(Paths.get("./target", "index_topic-0.offset"))
    dao = BgTestHelpers.dao(cassandraContainer.containerIpAddress, cassandraContainer.mappedPort(9042))
    irwService = IRWService.newIRW(dao, 25, true, 120.seconds)
    zStore = ZStore(dao)
    offsetsService = new ZStoreOffsetsService(zStore)
    val ftsOverridesConfig = BgTestHelpers.ftsOverridesConfig(elasticsearchContainer.containerIpAddress, elasticsearchContainer.mappedPort(9300))
    ftsServiceES = FailingFTSServiceMockup(ftsOverridesConfig , 2)
    BgTestHelpers.initFTSService(ftsServiceES)
    bgConfig = ftsOverridesConfig
      .withValue("cmwell.bg.kafka.bootstrap.servers", ConfigValueFactory.fromAnyRef(s"${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)}"))
    actorSystem = ActorSystem("cmwell-bg-test-system")
    cmwellBGActor = actorSystem.actorOf(CMWellBGActor.props(0, bgConfig, irwService, ftsServiceES, zStore, offsetsService))
    okToStartPromise.success(())
    super.beforeAll
  }


  describe("CmwellBG should") {

    val useNewlyCreatedAsBaseInfoton = okToStartPromise.future.flatMap { _ =>

      val systemFields = genericSystemFields.copy(lastModifiedBy = "Baruch")

      val pRecords = Seq.tabulate(20) { n =>
        val infoton = ObjectInfoton(systemFields.copy(path = s"/cmt/cm/bg-test/baseInfoton/info$n"),
          fields = Some(Map("a" -> Set(FieldValue("b"), FieldValue("c")))))
        val writeCommand = WriteCommand(infoton)
        val commandBytes = CommandSerializer.encode(writeCommand)
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      } :+ {
        val infoton = ObjectInfoton(systemFields.copy(path = s"/cmt/cm/bg-test/baseInfoton/info19"),
          fields = Some(Map("a1" -> Set(FieldValue("b1"), FieldValue("c1")))))
        val writeCommand = WriteCommand(infoton)
        val commandBytes = CommandSerializer.encode(writeCommand)
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      // send them all
      val sendEm = Future.traverse(pRecords)(sendToKafkaProducer)

      sendEm.flatMap{ _ =>
        cmwell.util.concurrent.spinCheck(250.millis,true,30.seconds){
          ftsServiceES.search(
            pathFilter = Some(PathFilter("/cmt/cm/bg-test/baseInfoton", true)),
            fieldsFilter = Some(FieldFilter(Must, Equals, "system.lastModifiedBy", "Baruch")),
            datesFilter = None,
            paginationParams = DefaultPaginationParams,
            sortParams = SortParam("system.indexTime" -> Asc),
            withHistory = false,
            withDeleted = false
          )
        }(_.total == 20)
      }.map { searchResults =>
        withClue(searchResults) {
          searchResults.length should be(20)
        }
      }
    }

    def afterFirst[T](body: =>Future[T])(implicit ec: ExecutionContext): Future[T] = executeAfterCompletion(useNewlyCreatedAsBaseInfoton)(body)(ec)

    //Assertions
    val writeCommandsProccessing = afterFirst{

      // prepare sequence of writeCommands
      val writeCommands = Seq.tabulate(10) { n =>
        val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test1/info$n"),
          fields = Some(Map("country" -> Set(FieldValue("Egypt"), FieldValue("Israel")))))
        WriteCommand(infoton)
      }

      // make kafka records out of the commands
      val pRecords = writeCommands.map { writeCommand =>
        val commandBytes = CommandSerializer.encode(writeCommand)
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      // send them all
      val sendEm = Future.traverse(pRecords)(sendToKafkaProducer)

      sendEm.flatMap { recordMetaDataSeq =>
//        logger.info(s"waiting for 5 seconds for $recordMetaDataSeq")
        cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds){
          irwService.readPathAsync("/cmt/cm/bg-test1/info1", ConsistencyLevel.QUORUM)
        } (!_.isEmpty).map { infopt =>
            infopt should not be empty
          }
        }
      }

    val processDeletePathCommands = executeAfterCompletion(writeCommandsProccessing){
      val deletePathCommand = DeletePathCommand("/cmt/cm/bg-test1/info0", lastModifiedBy = "Danny")
      val serializedCommand = CommandSerializer.encode(deletePathCommand)
      val pRecord = new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", serializedCommand)
      sendToKafkaProducer(pRecord).flatMap { recordMetaData =>
        cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds) {
          irwService.readPathAsync("/cmt/cm/bg-test1/info0", ConsistencyLevel.QUORUM)
        } {
          case FullBox(_: DeletedInfoton) => true
          case _ => false
        }.map {
            case FullBox(_: DeletedInfoton) => succeed
            case somethingElse => fail(s"expected a deleted infoton, but got [$somethingElse] from irw (recoredMetaData: $recordMetaData).")
          }
        }
      }

    val parentsCreation = executeAfterCompletion(writeCommandsProccessing){
      cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds) {
        Future.traverse(Seq("/cmt/cm/bg-test1", "/cmt/cm", "/cmt")) { path =>
        irwService.readPathAsync(path, ConsistencyLevel.QUORUM)
      }}(_.forall(!_.isEmpty)).map { infopts =>
        forAll(infopts) { infopt =>
          infopt should not be empty
        }
      }
    }

    val indexAllInfotons = processDeletePathCommands.flatMap { _ =>
      cmwell.util.concurrent.spinCheck(250.millis, true, 2.minutes) {
        ftsServiceES.search(
          pathFilter = Some(PathFilter("/cmt/cm/bg-test1", descendants = true)),
          fieldsFilter = None,
          datesFilter = None,
          paginationParams = DefaultPaginationParams,
          sortParams = SortParam.empty
        )
      }(_.total == 9)
    }
      .map { x =>
        withClue(s"$x") {
          x.total should equal(9)
        }
      }


    val groupedWriteCommands = afterFirst{

      val infotonPath = "/cmt/cm/bg-test/groupedWrites/info1"
      val currentTime = System.currentTimeMillis()
      val writeCommands = Seq.tabulate(20) { i =>
        val infoton = ObjectInfoton(genericSystemFields.copy(path = infotonPath, lastModified = new org.joda.time.DateTime(currentTime + i)),
          fields = Some(Map(s"f$i" -> Set(FieldValue(s"v$i")))))
        WriteCommand(infoton)
      }

      // make kafka records out of the commands
      val pRecords = writeCommands.map { writeCommand =>
        val commandBytes = CommandSerializer.encode(writeCommand)
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      // send them all
      val sendEm = Future.traverse(pRecords)(sendToKafkaProducer)

      sendEm.flatMap { recordMetaDataSeq =>
        val expectedFields = Seq.tabulate(20) { i =>
          s"f$i" -> Set(FieldValue(s"v$i"))
        }.toMap

        logger.info(s"waiting for 5 seconds for $recordMetaDataSeq")
        cmwell.util.concurrent.unsafeRetryUntil[cmwell.util.Box[Infoton]]({ bi =>
          bi.isDefined && bi.get.fields.fold(false)(_.size == 20)
        }, 30, 1.second) {
          irwService.readPathAsync(infotonPath, ConsistencyLevel.QUORUM)
        }.flatMap { infopt =>
          irwService.historyAsync(infotonPath, 20).flatMap { v =>
            irwService.readUUIDSAsync(v.map(_._2)).map { histories =>
              withClue(histories) {
                infopt should not be empty
                infopt.get.fields.get should contain theSameElementsAs expectedFields
              }
            }
          }
        }
      }
    }

    val indexTimeAddedToNonOverrideCmds = afterFirst {
      val writeCommands = Seq.tabulate(10) { n =>
        val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test/indexTime/info$n"),
          fields = Some(Map("a" -> Set(FieldValue("b"), FieldValue("c")))))
        WriteCommand(infoton)
      }

      // make kafka records out of the commands
      val pRecords = writeCommands.map { writeCommand =>
        val commandBytes = CommandSerializer.encode(writeCommand)
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      // send them all
      val sendEm = Future.traverse(pRecords)(sendToKafkaProducer)

      sendEm.flatMap { _ =>

        val searchQuery = cmwell.util.concurrent.spinCheck(250.millis, true, 1.minute) {
          ftsServiceES.search(
            pathFilter = Some(PathFilter("/cmt/cm/bg-test/indexTime", true)),
            fieldsFilter = Some(FieldFilter(Must, Equals, "system.lastModifiedBy", "Baruch")),
            datesFilter = None,
            paginationParams = DefaultPaginationParams,
            sortParams = SortParam("system.indexTime" -> Asc),
            withHistory = false,
            withDeleted = false
          )}{_.infotons.size == 10}

        def getQuery(infotons : Seq[Infoton]) = {
          Future.traverse(infotons) { i =>
            cmwell.util.concurrent.spinCheck(250.millis, true, 1.minute) {
              irwService.readUUIDAsync(i.uuid)
            }{res => res.nonEmpty && res.get.systemFields.indexName.nonEmpty}
          }
        }

        for {
          searchRes <- searchQuery
          getRes <- getQuery(searchRes.infotons)
        } yield {
          withClue((searchRes, getRes)) {
            val ftsSortedPaths = searchRes.infotons.map(_.systemFields.path)
            val irwSortedPaths = getRes.sortBy(_.get.systemFields.indexTime.get).map(_.get.systemFields.path)
            ftsSortedPaths should contain theSameElementsInOrderAs irwSortedPaths
          }
        }
      }
    }

    val markInfotonAsHistory = executeAfterCompletion(indexAllInfotons) {
      val writeCommand =
        WriteCommand(ObjectInfoton(genericSystemFields.copy(path = "/cmt/cm/bg-test1/info1", lastModifiedBy = "Hanna"),
          Map("i" -> Set(FieldValue("phone")))))

      val pRecord = new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", CommandSerializer.encode(writeCommand))
      sendToKafkaProducer(pRecord).flatMap { recordMetadata =>

        val f1 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds) (ftsServiceES.search(
            pathFilter = None,
            fieldsFilter = Some(MultiFieldFilter(Must, Seq(
              FieldFilter(Must, Equals, "system.path", "/cmt/cm/bg-test1/info1"),
              FieldFilter(Must, Equals, "system.current", "false"),
              FieldFilter(MustNot, Equals, "system.lastModifiedBy", "Hanna")))),
            datesFilter = None,
            paginationParams = DefaultPaginationParams,
            withHistory = true
          )){_.infotons.size == 1}

        val f2 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds) (ftsServiceES.search(
          pathFilter = None,
          fieldsFilter = Some(MultiFieldFilter(Must, Seq(
            FieldFilter(Must, Equals, "system.path", "/cmt/cm/bg-test1/info1"),
            FieldFilter(Must, Equals, "system.current", "true"),
            FieldFilter(Must, Equals, "system.lastModifiedBy", "Hanna")))),
          datesFilter = None,
          paginationParams = DefaultPaginationParams
        )){_.infotons.size == 1}

        val f3 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds) (irwService.historyAsync("/cmt/cm/bg-test1/info1", 1000)
          ){_.size == 2}

        for {
          ftsSearchResponse1 <- f1
          ftsSearchResponse2 <- f2
          irwHistoryResponse <- f3
        } yield withClue(ftsSearchResponse1, ftsSearchResponse2, irwHistoryResponse){
            ftsSearchResponse1.infotons should have size 1
            ftsSearchResponse2.infotons should have size 1
            irwHistoryResponse should have size 2
        }

      }
    }

    // ignored test position. to halt test from this point on,
    // we might want to depend on a different future than `okToStartPromise.future`
    // something more like: `afterStopAndStartPromise.future`

    val processOverrideCommands = afterFirst{
      val currentTime = System.currentTimeMillis()
      val numOfInfotons = 10 //starting from 0 up to 9
      val overrideCommands = Seq.tabulate(numOfInfotons) { n =>
        val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test3/info$n", indexTime = Some(currentTime + n + 1),
          lastModified = new org.joda.time.DateTime(currentTime + n), lastModifiedBy = "Ori"),
          fields = Some(Map("pearls" -> Set(FieldValue("Ubuntu"), FieldValue("shmubuntu")))))
        OverwriteCommand(infoton)
      }

      // make kafka records out of the commands
      val pRecords = overrideCommands.map { writeCommand =>
        val commandBytes = CommandSerializer.encode(writeCommand)
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      // send them all
      val sendEm = Future.traverse(pRecords)(sendToKafkaProducer)

      sendEm.flatMap { recordMetaDataSeq =>
        cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds)
        {ftsServiceES.search(
          pathFilter = Some(PathFilter("/cmt/cm/bg-test3", true)),
          fieldsFilter = Some(FieldFilter(Must, Equals, "system.lastModifiedBy", "Ori")),
          datesFilter = None,
          paginationParams = DefaultPaginationParams)}
        {response =>
          response.infotons.forall { infoton =>
            val l = infoton.systemFields.path.takeRight(1).toLong + 1L
            infoton.systemFields.indexTime.value == (currentTime + l)
          }
        }.map { response =>

          withClue(response, recordMetaDataSeq) {
            forAll(response.infotons) { infoton =>
              withClue(infoton) {
                val l = infoton.systemFields.path.takeRight(1).toLong + 1L
                infoton.systemFields.indexTime.value should be(currentTime + l)
              }
            }
          }
        }
      }
    }

    val reProcessNotIndexedOWCommands = afterFirst {
      val currentTime = System.currentTimeMillis()
      val infotons = Seq.tabulate(5) { n =>
        ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test/re_process_ow/info_override", indexTime = Some(currentTime + n * 3),
          lastModified = new DateTime(currentTime + n), lastModifiedBy = "Nahum", indexName = "cm_well_p0_0"),
          fields = Some(Map(s"a$n" -> Set(FieldValue(s"b$n"), FieldValue(s"c${n % 2}"))))
        )
      }
      val owCommands = infotons.map { i => OverwriteCommand(i) }

      // make kafka records out of the commands
      val pRecords = owCommands.map { writeCommand =>
        val commandBytes = CommandSerializer.encode(writeCommand)
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      // send them all
      val sendEm = Future.traverse(pRecords)(sendToKafkaProducer)
      sendEm.flatMap { _ =>
        ftsServiceES.deleteInfotons(infotons).flatMap { _ =>
          val sendAgain = Future.traverse(pRecords)(sendToKafkaProducer)
          sendAgain.flatMap { _ =>
            cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds) {
              ftsServiceES.search(
                pathFilter = Some(PathFilter("/cmt/cm/bg-test/re_process_ow", descendants = true)),
                fieldsFilter = Some(FieldFilter(Must, Equals, "system.lastModifiedBy", "Nahum")),
                datesFilter = None,
                paginationParams = DefaultPaginationParams,
                sortParams = FieldSortParams(List("system.indexTime" -> Desc)),
                withHistory = false,
                debugInfo = true)
            }(res => res.infotons.size == 1 && res.infotons.head.systemFields.lastModified.getMillis == currentTime + 4)
              .map { res =>
                withClue(res, res.infotons.headOption.fold(-1L)(_.systemFields.lastModified.getMillis), currentTime) {
                  res.infotons.size should equal(1)
                  res.infotons.head.systemFields.lastModified.getMillis should equal(currentTime + 4)
                }
              }
          }
        }
      }
    }

    val notGroupingOverrideCommands = afterFirst {
      val numOfInfotons = 15
      val overrideCommands = Seq.tabulate(numOfInfotons) { n =>
        val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test/override_not_grouped/info_override", indexTime = Some(Random.nextLong()),
          lastModifiedBy = "Ohad"), fields = Some(Map(s"Version${n % 3}" -> Set(FieldValue(s"a$n"), FieldValue(s"b${n % 2}")))))
        OverwriteCommand(infoton)
      }

      // make kafka records out of the commands
      val pRecords = overrideCommands.map { writeCommand =>
        val commandBytes = CommandSerializer.encode(writeCommand)
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      // send them all
      val sendEm = Future.traverse(pRecords)(sendToKafkaProducer)

      sendEm.flatMap { _ =>
        cmwell.util.concurrent.spinCheck(250.millis, true, 2.minutes) {
          ftsServiceES.search(
            pathFilter = Some(PathFilter("/cmt/cm/bg-test/override_not_grouped", false)),
            fieldsFilter = Some(FieldFilter(Must, Equals, "system.lastModifiedBy", "Ohad")),
            datesFilter = None,
            paginationParams = DefaultPaginationParams,
            withHistory = true,
            debugInfo = true)
        }(_.total == numOfInfotons)
          .map { res =>
            withClue(res) {
              res.total should equal(numOfInfotons)
            }
          }
      }
    }


    val persistAndIndexLargeInfoton = afterFirst {

      val lotsOfFields = Seq.tabulate(8000) { n =>
        s"field$n" -> Set[FieldValue](FString(s"value$n"))
      }.toMap

      val fatFoton = ObjectInfoton(genericSystemFields.copy(path = "/cmt/cm/bg-test-fat/fatfoton1", dc = "dcc", lastModifiedBy = "Faruk"), lotsOfFields)

      // make kafka record out of the infoton
      val pRecord = {
        val commandBytes = CommandSerializer.encode(OverwriteCommand(fatFoton))
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      val sendIt = sendToKafkaProducer(pRecord)

      sendIt.flatMap { _ =>
        cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds) {
          val readReply = irwService.readPathAsync("/cmt/cm/bg-test-fat/fatfoton1")
          val searchReply = ftsServiceES.search(
            pathFilter = Some(PathFilter("/cmt/cm/bg-test-fat", true)),
            fieldsFilter = Some(FieldFilter(Must, Equals, "system.lastModifiedBy", "Faruk")),
            datesFilter = None,
            paginationParams = DefaultPaginationParams)
          readReply.zip(searchReply)
        } { case (readResult, searchResult) => readResult.nonEmpty && searchResult.infotons.exists(_.systemFields.path == "/cmt/cm/bg-test-fat/fatfoton1") }
          .map { case tuple@(readResult, searchResult) =>
            withClue(tuple) {
              readResult should not be empty
              val paths = searchResult.infotons.map(_.systemFields.path)
              paths should contain("/cmt/cm/bg-test-fat/fatfoton1")
            }
          }
      }
    }

    val deeplyNestedOverrideCommands = afterFirst{
      val currentTime = System.currentTimeMillis()
      val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test4/deeply/nested/overwrite/infobj", indexTime = Some(currentTime + 42),
        lastModifiedBy = "Noga"), fields = Some(Map("whereTo" -> Set(FieldValue("The"), FieldValue("ATM!")))))

      // make kafka record out of the infoton
      val pRecord = {
        val commandBytes = CommandSerializer.encode(OverwriteCommand(infoton))
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }

      val sendIt = sendToKafkaProducer(pRecord)

      sendIt.flatMap { recordMetaData=>
          val f0 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds)(ftsServiceES.search(
            pathFilter = Some(PathFilter("/cmt/cm/bg-test4", descendants = true)),
            fieldsFilter = Some(FieldFilter(Must, Equals, "system.lastModifiedBy", "Noga")),
            datesFilter = None,
            paginationParams = DefaultPaginationParams)) { res =>
            val paths = res.infotons.map(_.systemFields.path)
            (paths.size == 1) && (!paths.contains("/cmt/cm/bg-test4/deeply")) && (!paths.contains("/cmt/cm/bg-test4/deeply/nested")) &&
              (!paths.contains("/cmt/cm/bg-test4/deeply/nested/overwrite")) && paths.contains("/cmt/cm/bg-test4/deeply/nested/overwrite/infobj")
          }

          val f1 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds)(
            irwService.readPathAsync("/cmt/cm/bg-test4", ConsistencyLevel.QUORUM)
          ) {
            _.isEmpty
          }

          val f2 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds)(
            irwService.readPathAsync("/cmt/cm/bg-test4/deeply", ConsistencyLevel.QUORUM)
          ) {
            _.isEmpty
          }

          val f3 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds)(
            irwService.readPathAsync("/cmt/cm/bg-test4/deeply/nested", ConsistencyLevel.QUORUM)
          ) {
            _.isEmpty
          }

          val f4 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds)(
            irwService.readPathAsync("/cmt/cm/bg-test4/deeply/nested/overwrite", ConsistencyLevel.QUORUM)
          ) {
            _.isEmpty
          }

          val f5 = cmwell.util.concurrent.spinCheck(250.millis, true, 30.seconds)(
            irwService.readPathAsync("/cmt/cm/bg-test4/deeply/nested/overwrite/infobj", ConsistencyLevel.QUORUM)
          ) {
            !_.isEmpty
          }

          for {
            r0 <- f0
            r1 <- f1
            r2 <- f2
            r3 <- f3
            r4 <- f4
            r5 <- f5
          } yield withClue(r0, r1, r2, r3, r4, r5, recordMetaData) {
            val paths = r0.infotons.map(_.systemFields.path)
            paths shouldNot contain("/cmt/cm/bg-test4/deeply")
            paths shouldNot contain("/cmt/cm/bg-test4/deeply/nested")
            paths shouldNot contain("/cmt/cm/bg-test4/deeply/nested/overwrite")
            paths should contain("/cmt/cm/bg-test4/deeply/nested/overwrite/infobj")
            paths should have size 1
            r1 shouldBe empty
            r2 shouldBe empty
            r3 shouldBe empty
            r4 shouldBe empty
            r5 should not be empty
          }

      }
    }

    val currentTime = System.currentTimeMillis()
    def sendIt(i: Infoton): Future[RecordMetadata] = {
      val pRecord = {
        val commandBytes = CommandSerializer.encode(OverwriteCommand(i))
        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
      }
      sendToKafkaProducer(pRecord)
    }
    def verifyBgTest5(modifier: String) = {
      val f0 = ftsServiceES.search(
        pathFilter = None,
        fieldsFilter = Some(MultiFieldFilter(Must, Seq(
          FieldFilter(Must, Equals, "system.path", "/cmt/cm/bg-test5/infobj"),
          FieldFilter(Must, Equals, "system.lastModifiedBy", modifier)))),
        datesFilter = None,
        paginationParams = DefaultPaginationParams,
        withHistory = false
      )
      val f1 = ftsServiceES.search(
        pathFilter = None,
        fieldsFilter = Some(MultiFieldFilter(Must, Seq(
          FieldFilter(Must, Equals, "system.path", "/cmt/cm/bg-test5/infobj"),
          FieldFilter(Must, Equals, "system.current", "false")))),
        datesFilter = None,
        paginationParams = DefaultPaginationParams,
        withHistory = true
      )

      val f2 = irwService.readPathAsync("/cmt/cm/bg-test5/infobj", ConsistencyLevel.QUORUM)
      val f3= irwService.historyAsync("/cmt/cm/bg-test5/infobj", 10)
      for {
        r0 <- f0
        r1 <- f1
        r2 <- f2
        r3 <- f3
      } yield (r0,r1,r2,r3)
    }

    def waitForIt(numOfVersionsToExpect: Int)(implicit ec: ExecutionContext): Future[FTSSearchResponse] = {
      val startTime = System.currentTimeMillis()
      def waitForItInner(): Future[FTSSearchResponse] = {
        ftsServiceES.search(
          pathFilter = None,
          fieldsFilter = Some(FieldFilter(Must, Equals, "system.path", "/cmt/cm/bg-test5/infobj")),
          datesFilter = None,
          paginationParams = DefaultPaginationParams,
          withHistory = true
        )(ec,logger).flatMap { res =>
          if (res.total >= numOfVersionsToExpect) Future.successful(res)
          else if(System.currentTimeMillis() - startTime > 30000L) Future.failed(new IllegalStateException(s"Waited for over 30s, last res: ${res.toString}"))
          else scheduleFuture(1.second)(waitForItInner())
        }(ec)
      }
      waitForItInner()
    }

    val version3IngestAndVerify = afterFirst {
       val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test5/infobj", indexTime = Some(currentTime + 42), lastModifiedBy = "Vicki3",
         lastModified = new org.joda.time.DateTime(currentTime) ,dc = "dc"), fields = Some(Map("GoTo" -> Set(FieldValue("draw"), FieldValue("money")))))

      sendIt(infoton).flatMap { recordMetaData =>
        waitForIt(1).flatMap { ftsRes =>
          verifyBgTest5("Vicki3").map {
            case t@(currFTSRes, histFTSRes, currPathIRWBox, historiesIRW) => withClue(t -> ftsRes) {
              val currPathIRW = currPathIRWBox.toOption
              currFTSRes.total should be(1)
              currFTSRes.infotons.head.systemFields.indexTime.value should be(currentTime + 42)
              histFTSRes.total should be(0)
              currPathIRW shouldBe defined
              currPathIRW.value.systemFields.indexTime.value should be(currentTime + 42)
              currPathIRW.value.uuid shouldEqual currFTSRes.infotons.head.uuid
              historiesIRW should have size 1
            }
          }
        }
      }
    }

    val version1IngestAndVerify = executeAfterCompletion(version3IngestAndVerify){
       val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test5/infobj", lastModified = new org.joda.time.DateTime(currentTime - 20000),
         lastModifiedBy = "Vicki5", dc = "dc"), fields = Some(Map("whereTo" -> Set(FieldValue("Techno"), FieldValue("Doron")))))

      sendIt(infoton).flatMap { recordMetaData =>
        waitForIt(2).flatMap { ftsRes =>
          verifyBgTest5("Vicki3").map {
            case t@(currFTSRes, histFTSRes, currPathIRWBox, historiesIRW) => withClue(t -> ftsRes) {
              val currPathIRW = currPathIRWBox.toOption
              currFTSRes.total should be(1)
              currFTSRes.infotons.head.systemFields.indexTime.value should be(currentTime + 42)
              histFTSRes.total should be(1)
              currPathIRW shouldBe defined
              currPathIRW.value.systemFields.indexTime.value should be(currentTime + 42)
              currPathIRW.value.uuid shouldEqual currFTSRes.infotons.head.uuid
              historiesIRW should have size 2
            }
          }
        }
      }
    }

    val version2IngestAndVerify = executeAfterCompletion(version1IngestAndVerify){
       val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test5/infobj", dc = "dc", indexTime = Some(currentTime + 128),
         lastModified = new org.joda.time.DateTime(currentTime - 10000), lastModifiedBy = "Vicki2"),
        fields = Some(Map("OK" -> Set(FieldValue("TO"), FieldValue("ATM!")))))

      sendIt(infoton).flatMap { recordMetaData =>
        waitForIt(3).flatMap { ftsRes =>
          verifyBgTest5("Vicki3").map {
            case t@(currFTSRes,histFTSRes,currPathIRWBox,historiesIRW) => withClue(t -> ftsRes) {
              val currPathIRW = currPathIRWBox.toOption
              currFTSRes.total should be(1)
              currFTSRes.infotons.head.systemFields.indexTime.value should be(currentTime + 42)
              histFTSRes.total should be(2)
              currPathIRW shouldBe defined
              currPathIRW.value.systemFields.indexTime.value should be(currentTime + 42)
              currPathIRW.value.uuid shouldEqual currFTSRes.infotons.head.uuid
              historiesIRW should have size 3
            }
          }
        }
      }
    }

    val version4IngestAndVerify = executeAfterCompletion(version2IngestAndVerify){
       val infoton = ObjectInfoton(genericSystemFields.copy(path = s"/cmt/cm/bg-test5/infobj", dc = "dc", indexTime = Some(currentTime + 23),
         lastModified = new org.joda.time.DateTime(currentTime + 10000), lastModifiedBy = "Vicki4"),
        fields = Some(Map("No" -> Set(FieldValue("U"), FieldValue("Go")),
                          "But" -> Set(FieldValue("I don't need the ATM.")))))

      sendIt(infoton).flatMap { recordMetaData =>
        val waitRes = cmwell.util.concurrent.spinCheck(250.millis, true, 1.minutes) {
          ftsServiceES.search(
            pathFilter = None,
            fieldsFilter = Some(FieldFilter(Must, Equals, "system.path", "/cmt/cm/bg-test5/infobj")),
            datesFilter = None,
            paginationParams = DefaultPaginationParams,
            withHistory = true
          )
        }(_.total >= 4)

        waitRes.flatMap { ftsRes =>
          verifyBgTest5("Vicki4").map {
            case t@(currFTSRes, histFTSRes, currPathIRWBox, historiesIRW) => withClue(t -> ftsRes) {
              val currPathIRW = currPathIRWBox.toOption
              currFTSRes.total should be(1)
              currFTSRes.infotons.head.systemFields.indexTime.value should be(currentTime + 23)
              histFTSRes.total should be(3)
              currPathIRW shouldBe defined
              currPathIRW.value.systemFields.indexTime.value should be(currentTime + 23)
              currPathIRW.value.uuid shouldEqual currFTSRes.infotons.head.uuid
              historiesIRW should have size 4
            }
          }
        }
      }
    }

    val version5IngestAndVerify = executeAfterCompletion(version4IngestAndVerify){
       val infoton = ObjectInfoton(genericSystemFields.copy(path =s"/cmt/cm/bg-test5/infobj", indexTime = Some(currentTime + 1729),
         lastModified = new org.joda.time.DateTime(currentTime + 20000), lastModifiedBy = "Vicki5"),
        fields = Some(Map("So" -> Set(FieldValue("Why you asked?")), "Ummm" -> Set(FieldValue("I didn't...")))))

      sendIt(infoton).flatMap { recordMetaData =>
        waitForIt(5).flatMap { ftsRes =>
          verifyBgTest5("Vicki5").map {
            case t@(currFTSRes,histFTSRes,currPathIRWBox,historiesIRW) => withClue(t -> ftsRes) {
              val currPathIRW = currPathIRWBox.toOption
              currFTSRes.total should be(1)
              currFTSRes.infotons.head.systemFields.indexTime.value should be(currentTime + 1729)
              histFTSRes.total should be(4)
              currPathIRW shouldBe defined
              currPathIRW.value.systemFields.indexTime.value should be(currentTime + 1729)
              currPathIRW.value.uuid shouldEqual currFTSRes.infotons.head.uuid
              historiesIRW should have size 5
            }
          }
        }
      }
    }


    it("use the cache of newly created infotons as baseInfoton before merge")(useNewlyCreatedAsBaseInfoton)
    it("process WriteCommands")(writeCommandsProccessing)
    it("process DeletPathCommands")(processDeletePathCommands)
    it("create parents")(parentsCreation)
    it("index all processed infotons")(indexAllInfotons)
    it("add index time to non override commands and update indexTime in Cassandra")(indexTimeAddedToNonOverrideCmds)
    it("mark infoton as history if newer version is sent for it")(markInfotonAsHistory)
    it("process WriteCommands containing fat infoton")(persistAndIndexLargeInfoton)
//    ignore("continue from where it has stopped after stopping and starting it again"){
//
//      // stop BG
//      logger debug "sending stop message to cmwellBGActor"
//      val stopReply = Await.result(ask(cmwellBGActor, Stop)(30.seconds).mapTo[Stopped.type], 30.seconds)
//
//      val numOfInfotons = 8
//      val writeCommands = Seq.tabulate(numOfInfotons) { n =>
//        val infoton = ObjectInfoton(
//          path = s"/cmt/cm/bg-test2/info$n",
//          dc = "dc",
//          indexTime = None,
//          fields = Some(Map("food" -> Set(FieldValue("Malabi"), FieldValue("Brisket")))))
//        WriteCommand(infoton)
//      }
//
//      // make kafka records out of the commands
//      val pRecords = writeCommands.map { writeCommand =>
//        val commandBytes = CommandSerializer.encode(writeCommand)
//        new ProducerRecord[Array[Byte], Array[Byte]]("persist_topic", commandBytes)
//      }
//
//      // send them all
//      pRecords.foreach(kafkaProducer.send)
//
//      // restart bg
//      val startReply = Await.result(ask(cmwellBGActor, Start)(10.seconds).mapTo[Started.type], 10.seconds)
//
//      Thread.sleep(5000)
//
//      for (i <- 0 until numOfInfotons) {
//        val readPath = Await.result(irwService.readPathAsync(s"/cmt/cm/bg-test2/info$i", ConsistencyLevel.QUORUM), 5.seconds)
//        withClue(readPath) {
//          readPath should not be empty
//        }
//      }
//
//      Await.result(ftsServiceES.search(
//        pathFilter = Some(PathFilter("/cmt/cm/bg-test2", true)),
//        fieldsFilter = None,
//        datesFilter = None,
//        paginationParams = DefaultPaginationParams
//      ), 5.seconds).total should equal(numOfInfotons)
//
//    }
    it("re process OW commands even if were not indexed at first")(reProcessNotIndexedOWCommands)
    it("process OverrideCommands correctly by keeping its original indexTime and not generating a new one")(processOverrideCommands)
    // scalastyle:off
    it("process group of writecommands in short time while keeping all fields (in case of the data being splitted to several versions, last version must contain all data)")(groupedWriteCommands)
    // scalastyle:on
    it("process OverrideCommands correctly by not grouping same path commands together for merge")(notGroupingOverrideCommands)
    it("process OverrideCommands correctly by creating parents if needed")(deeplyNestedOverrideCommands)
    describe("process OverrideCommands correctly by keeping history in correct order") {
      it("while ingesting version 3 first, and verifying  version 3 is current")(version3IngestAndVerify)
      it("and then ingesting version 1 and verifying version 1 is history, while version 3 stays current")(version1IngestAndVerify)
      // scalastyle:off
      it("and then ingesting version 2, which is history but with newer indexTime, and verifying version 2&1 are history, while version 3 stays current")(version2IngestAndVerify)
      // scalastyle:on
      it("and then ingesting version 4 with older indexTime and verifying version 1-3 are history, while version 4 became current")(version4IngestAndVerify)
      it("and then ingesting version 5 and verifying version 1-4 are history, while version 5 became current")(version5IngestAndVerify)
    }
//    describe("not generate duplicates, no matter how many consecutive updates occur on same path") {
//
//    }
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
