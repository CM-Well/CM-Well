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


package cmwell.imp

import collection.mutable
import java.io.File

import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Suite}
import cmwell.tlog.{TLog, TLogState}
import cmwell.domain._
import cmwell.common._
import cmwell.driver.{Dao, DaoExecution}
import cmwell.common.DeleteAttributesCommand
import cmwell.common.WriteCommand
import cmwell.domain.FString
import cmwell.domain.FInt
import cmwell.irw.IRWService
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import k.grid._

import scala.util.{Failure, Success, Try}
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created with IntelliJ IDEA.
 * User: markz
 * Date: 12/10/12
 * Time: 8:05 PM
 *
 */
trait IMPServiceTest extends BeforeAndAfterAll { this:Suite =>

  var tlogCass:TLog = _
  var uuidLogCass: TLog = _
  var dao : Dao = _
  var irw : IRWService = _
  var imp : IMPServiceImpl = _
  var impState : TLogState = _

  override protected def beforeAll() {
    super.beforeAll()

    def init = {
      System.setProperty("cmwell.home", new File(".").getCanonicalPath + File.separator + "target")
      System.setProperty("dataCenter.id", "dc_test")

      dao = Dao("Test", "data")

      // in order for irw and imp to invoke new ZStore, "data2" must exist:
      implicit val daoProxy = dao
      new DaoExecution {
        def addKeyspaceData2() = {
          val stmt = dao.getSession.prepare("CREATE KEYSPACE IF NOT EXISTS data2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };").bind
          executeAsyncInternal(stmt)
        }
      }.addKeyspaceData2


      tlogCass = TLog("TLog", "w1")
      uuidLogCass = TLog("UuidLog", "w1")
      tlogCass.init()
      uuidLogCass.init()
      // create infoton read write service
      irw = IRWService(dao, 25, true, 120.seconds)

      impState = TLogState("imp", "TLog", "w1")
      // create imp service
      imp = IMPService(tlogCass, uuidLogCass, irw, impState)

      Grid.setGridConnection(GridConnection(memberName = "imp", hostName = "127.0.0.1", port = 7777, seeds = Set("127.0.0.1:7777"), clusterName = "localTest"))
      Grid.join
    }

    while(Try(init).isFailure) {
      Thread.sleep(250)
    }
  }

  override protected def afterAll() {
    tlogCass.shutdown()
    uuidLogCass.shutdown()
    dao.shutdown()
    Grid.shutdown
    super.afterAll()
  }
}

class IMPCassSpec extends FlatSpec with Matchers with IMPServiceTest {

  val logger = {
    import org.slf4j.LoggerFactory
    com.typesafe.scalalogging.Logger(LoggerFactory.getLogger("ROOT"))
  }
  val timeToWait = 1000
  val waitDuration = timeToWait.millis

  "update command on a fresh directory" should "be successful" in {
    val m : Map[String , Set[FieldValue]]= Map("name" -> Set(FString("gal"), FString("yoav")), "types" -> Set(FString("123"), FInt(123)))
    val cmdUpdate = UpdatePathCommand("/cmd/p1/p2/p3/update" , Map.empty[String , Set[FieldValue]], m , new DateTime )
    val payload : Array[Byte] = CommandSerializer.encode(cmdUpdate)

    tlogCass.write(payload)
    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process
    // check parents
    val p = Vector("/cmd/p1/p2/p3/update","/cmd/p1/p2/p3","/cmd/p1/p2","/cmd/p1","/cmd")
    val ps =  Await.result(irw.readPathsAsync(p), waitDuration)
    ps.size should equal (p.size)
  }

  "ignore alredy deleted infoton" should "be successful" in {
    val m : Map[String , Set[FieldValue]]= Map("name" -> Set(FString("gal"), FString("yoav")), "types" -> Set(FString("123"), FInt(123)))
    val obj = ObjectInfoton("/cmd/delete/dupdeltest","dc_test",None,m)

    val cmdWrite = WriteCommand(obj)
    val payload : Array[Byte] = CommandSerializer.encode(cmdWrite)
    tlogCass.write(payload)
    Thread.sleep(timeToWait * 10)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    val cmdDelete1 = DeletePathCommand("/cmd/delete/dupdeltest" , new DateTime())
    tlogCass.write(CommandSerializer.encode(cmdDelete1))
    Thread.sleep(timeToWait * 10)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    val cmdDelete2 = DeletePathCommand("/cmd/delete/dupdeltest" , new DateTime())
    tlogCass.write(CommandSerializer.encode(cmdDelete2))
    Thread.sleep(timeToWait * 10)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process


    val paths = irw.history("/cmd/delete/dupdeltest",100000)
    paths.size should equal (2)
  }

  "write and merge" should "be successful" in {

    var data : mutable.Buffer[Infoton] = new mutable.ListBuffer[Infoton]()

    // create 1000 object infotons
    for ( i <- 0 until 10 ) {
      val m : Map[String , Set[FieldValue]]= Map("name" -> Set(FString("gal"), FString("yoav")), "types" -> Set(FString("123"), FInt(123)))
      val objInfo = ObjectInfoton("/cmt/cm/command-test/objinfo_" + i,"dc_test", None, m)
      data += objInfo
    }

    // iterate buffer infoton and write them to TLog
    for ( item <- data ) {
      // create write command
      val cmdWrite = WriteCommand(item)
      val payload : Array[Byte] = CommandSerializer.encode(cmdWrite)
      tlogCass.write(payload)
    }
    Thread.sleep(timeToWait);
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    // check if can real all infotons
    for ( item <- data ) {
      val fields = item.fields.get
      val typeCheck1 = fields("types").exists(_.isInstanceOf[FString])
      val typeCheck2 = fields("types").exists(_.isInstanceOf[FInt])
      val info = Await.result(irw.readUUIDAsync(item.uuid),waitDuration)
      info match {
        case FullBox(i) => { item.path  should equal (i.path);
          item.uuid  should equal (i.uuid);
          item.lastModified.getMillis should equal (i.lastModified.getMillis);
          typeCheck1 should equal (true)
          typeCheck2 should equal (true)
        }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }
    // now lets check the read path method
    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => { item.path  should equal (i.path);item.uuid  should equal (i.uuid);item.lastModified.getMillis should equal (i.lastModified.getMillis); }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }

    data = new mutable.ListBuffer[Infoton]()
    // add a new kid name
    for ( i <- 0 until 10 ) {
      val m : Map[String , Set[FieldValue]] = Map("name" -> Set(FString("roni")))
      val objInfo = ObjectInfoton("/cmt/cm/command-test/objinfo_" + i ,"dc_test", None, m)
      data += objInfo
    }

    // iterate buffer infoton and write them to TLog
    for ( item <- data ) {
      // create write command
      val cmdWrite = WriteCommand(item)
      val payload : Array[Byte] = CommandSerializer.encode(cmdWrite)
      tlogCass.write(payload)
    }

    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    // now lets check the read path method
    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => { item.path  should equal (i.path);item.lastModified.getMillis should equal (i.lastModified.getMillis); 3 should equal (i.fields.get("name").size) }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }


    data = new mutable.ListBuffer[Infoton]()
    // add a new kid name
    for ( i <- 0 until 10 ) {
      val m : Map[String , Set[FieldValue]] = Map("last_name" -> Set(FString("smith")))
      val objInfo = ObjectInfoton("/cmt/cm/command-test/objinfo_" + i,"dc_test", None, m)
      data += objInfo
    }

    // iterate buffer infoton and write them to TLog
    for ( item <- data ) {
      // create write command
      val cmdWrite = WriteCommand(item)
      val payload : Array[Byte] = CommandSerializer.encode(cmdWrite)
      tlogCass.write(payload)
    }

    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    // now lets check the read path method
    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => { item.path  should equal (i.path);item.lastModified.getMillis should equal (i.lastModified.getMillis); 3 should equal (i.fields.get("name").size);1 should equal (i.fields.get("last_name").size) }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }

    // now lets delete
    data = new mutable.ListBuffer[Infoton]()

    for ( i <- 0 until 10 ) {
      val m : Map[String , Set[FieldValue]] = Map("name" -> Set(FString("gal"), FString("yoav")))
      val objInfo = ObjectInfoton( "/cmt/cm/command-test/objinfo_" + i,"dc_test", None, m)
      data += objInfo
    }

    for ( item <- data ) {
      // create write command
      val cmdDeleteAtrib = DeleteAttributesCommand(item.path,item.fields.get , new DateTime)
      val payload : Array[Byte] = CommandSerializer.encode(cmdDeleteAtrib)
      tlogCass.write(payload)
    }

    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => { item.path  should equal (i.path);1 should equal (i.fields.get("name").size);1 should equal (i.fields.get("last_name").size) }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }


    // now lets delete
    data = new mutable.ListBuffer[Infoton]()

    for ( i <- 0 until 10 ) {
      val m : Map[String , Set[FieldValue]] = Map("last_name" -> Set(FString("smith")))

      val objInfo = ObjectInfoton("/cmt/cm/command-test/objinfo_" + i ,"dc_test", None, m)
      data += objInfo
    }

    for ( item <- data ) {
      // create write command
      val cmdDeleteAttributes = DeleteAttributesCommand(item.path,item.fields.get, new DateTime)
      val payload : Array[Byte] = CommandSerializer.encode(cmdDeleteAttributes)
      tlogCass.write(payload)
    }

    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => {
          item.path should equal(i.path)
          1 should equal(i.fields.get("name").size)
          1 should equal(i.fields.size)
        }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }

    }

    data = new mutable.ListBuffer[Infoton]()

    for ( i <- 0 until 10 ) {
      val m : Map[String , Set[FieldValue]] = Map("L" -> Set(FString("U"),FString("I")),"Location" -> Set(FString("USA")),"COMPANY" -> Set(FString("IBM"),FString("FACEBOOK"),FString("GOOGLE")))
      val objInfo = ObjectInfoton("/cmt/cm/update/command-test/objinfo_" + i,"dc_test", None, m)
      data += objInfo
    }

    for ( item <- data ) {
      // create write command
      val cmdUpdate = UpdatePathCommand(item.path,Map.empty, item.fields.get, new DateTime)
      val payload : Array[Byte] = CommandSerializer.encode(cmdUpdate)
      tlogCass.write(payload)
    }

    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => {
          item.path should equal (i.path)
          val data = i.fields.get
          1 should equal ( data("Location").size )
          3 should equal ( data("COMPANY").size )
          2 should equal ( data("L").size )
        }
        case EmptyBox => fail(s"Option was empty - infoton could not be read from IRW for path ${item.path}")
        case BoxedFailure(e) => fail(e)
      }

    }

    val m = Await.result(Future.traverse(data){ item =>
      irw.readPathAsync(item.path).map(item.path -> _.toOption)
    }.map(_.toMap.withDefaultValue(None)),waitDuration*10)

    logger.info(s"infotons before ingesting: $m")

    for ( item <- data ) {
      // create write command
      val d_f : Map[String , Set[FieldValue]] = Map("L" -> Set.empty[FieldValue], "Location" -> Set(FString("USA")),"COMPANY" -> Set(FString("GOOGLE"),FString("FACEBOOK")) )
      val u_f : Map[String , Set[FieldValue]] = Map("COMPANY" -> Set(FString("WAZE")))

      val cmdUpdate = UpdatePathCommand(item.path,d_f, u_f, new DateTime)
      val payload : Array[Byte] = CommandSerializer.encode(cmdUpdate)
      tlogCass.write(payload)
    }

    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    for ( item <- data ) {
      //spin-check data is persisted.
      var flag = true
      var wait = waitDuration
      var increasingWait = 50.millis
      var cntr = 51
      do {
        logger.info(s"waiting for ${item.path} to be persisted.")
        val f = cmwell.util.concurrent.SimpleScheduler.scheduleFuture(increasingWait){
          irw.readPathAsync(item.path, cmwell.irw.QUORUM)
        }
        Try(Await.result(f,wait)) match {
          case Success(opt) => opt.foreach{ i =>
            m(i.path).foreach{ j =>
              logger.info(s"new infoton read from irw [${i.uuid}]: $i")
              logger.info(s"infoton stored in map [${j.uuid}]: $j")
              flag = i.fields.fold(true)(m => m.size != 1 && m.get("COMPANY").fold(true)(_.size != 2))
            }
          }
          case Failure(err) => logger.error(s"Await for irw.readPathAsync(${item.path}) failed",err)
        }
        cntr -= 1
        wait += waitDuration
        increasingWait += 50.millis
      } while(flag && cntr > 0)

      val info = Await.result(irw.readPathAsync(item.path, cmwell.irw.QUORUM),waitDuration)
      info match {
        case FullBox(i) => {
          logger.info(s"received infoton for path='${item.path}': $i")
          item.path  should equal (i.path)
          val data = i.fields.get
          withClue(i) {
            2 should equal ( data("COMPANY").size )
            1 should equal ( data.size )
          }
        }
        case EmptyBox => fail(s"did not receive infoton for path='${item.path}'")
        case BoxedFailure(e) => fail(e)
      }
    }

    data = new mutable.ListBuffer[Infoton]()

    for ( i <- 0 until 10 ) {
      val m : Map[String , Set[FieldValue]]= Map("name" -> Set(FString("gal"), FString("yoav")), "types" -> Set(FString("123"), FInt(123)))
      val objInfo = ObjectInfoton("/cmt/cm/command-test/delete/objinfo_" + i,"dc_test", None, m)
      data += objInfo
    }

    // iterate buffer infoton and write them to TLog
    for ( item <- data ) {
      // create write command
      val cmdWrite = WriteCommand(item)
      val payload : Array[Byte] = CommandSerializer.encode(cmdWrite)
      tlogCass.write(payload)
    }
    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    // check if can real all infotons
    for ( item <- data ) {
      val fields = item.fields
      val typeCheck1 = fields.get("types").toList(0).isInstanceOf[FString]
      val typeCheck2 = fields.get("types").toList(1).isInstanceOf[FInt]
      val info = Await.result(irw.readUUIDAsync(item.uuid),waitDuration)
      info match {
        case FullBox(i) => { item.path  should equal (i.path)
          item.uuid  should equal (i.uuid)
          item.lastModified.getMillis should equal (i.lastModified.getMillis)
          typeCheck1 should equal (true)
          typeCheck2 should equal (true)
        }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }
    // now lets check the read path method
    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => { item.path  should equal (i.path);item.uuid  should equal (i.uuid);item.lastModified.getMillis should equal (i.lastModified.getMillis) }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }


    // now lets delete all infotons
    for ( item <- data ) {
      val cmdDeletePath = DeletePathCommand(item.path , new DateTime)
      val payload : Array[Byte] = CommandSerializer.encode(cmdDeletePath)
      tlogCass.write(payload)

    }

    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => {
          item.path  should equal (i.path)
          i match {
            case d:DeletedInfoton =>
              true should equal (true)
            case _ =>
              true should equal (false)
          }
        }
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }

    // let's write
    for ( item <- data ) {
      // create write command
      val cmdWrite = WriteCommand(item)
      val payload : Array[Byte] = CommandSerializer.encode(cmdWrite)
      tlogCass.write(payload)
    }
    Thread.sleep(timeToWait)
    // do the merge and persist
    imp.stopAfterChunks = 1
    imp.process

    // now lets check the read path method
    for ( item <- data ) {
      val info = Await.result(irw.readPathAsync(item.path),waitDuration)
      info match {
        case FullBox(i) => item.path  should equal (i.path)
        case EmptyBox => fail("empty box")
        case BoxedFailure(e) => fail(e)
      }
    }

  }

}
