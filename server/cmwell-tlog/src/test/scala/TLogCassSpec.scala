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



import cmwell.domain._
import cmwell.tlog.{TLog, TLogState}
import cmwell.common.{CommandSerializer, WriteCommand}

import collection.mutable
import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.scalatest._

import scala.concurrent._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created with IntelliJ IDEA.
 * User: markz
 * Date: 12/6/12
 * Time: 9:37 AM
 * Test for TLog cass implementation.
 */

trait TLogServiceCassTest extends BeforeAndAfterAll with LazyLogging { this:Suite =>
  var tlogLin:TLog = _
  var tlogLinDummy:TLog = _
  var tlogPra: TLog = _
  var agentState : TLogState = _

  val numberOfChunks = 1000
  // on test do 200
  val chunkSize = 100
  val numberOfWrites = chunkSize * numberOfChunks

  override protected def beforeAll() {
    val cmwHome = System.getProperty("cmwell.home") match {
      case null => s"${new File(".").getCanonicalPath}${File.separator}target${File.separator}tlog-test"
      case s => s
    }

    System.setProperty("cmwell.home", cmwHome)
    tlogLin = TLog("payload", "w1", bucketSize = 1L * 1024L * 1024L)
    tlogLin.init()

    tlogPra = TLog("payload_p" ,"w1", bucketSize = 1L * 1024L * 1024L )
    tlogPra.init()


    agentState = TLogState("imp" , "payload" , "w1")
    val time = System.currentTimeMillis()
    logger.info("time: %s".format(time))
    Random.setSeed(time)
    super.beforeAll()
  }

  override protected def afterAll() {
    tlogLin.shutdown()
    tlogPra.shutdown()
    super.afterAll()
  }
}

class TLogCassSpec extends FlatSpec with Matchers with Inspectors with TLogServiceCassTest {


   "write to tlog commands" should "be successful" in {
     var data : mutable.Buffer[Infoton] = new mutable.ListBuffer[Infoton]()
     // init reader before writer
     val tlogCommandsWriter = TLog("payload_command", "w1", bucketSize = 1L * 1024L * 1024L)
     tlogCommandsWriter.init()

     val tlogCommandsReader = tlogCommandsWriter
     // create 10 object infotons
     for ( i <- 0 until numberOfWrites ) {
       val objInfo = ObjectInfoton("/command-test/objinfo" + i,"dc_test", None, Map("name" -> Set[FieldValue](FString("gal"), FString("yoav"))))
       data += objInfo
     }

     // iterate buffer infoton and write them to TLog
     for ( item <- data ) {
       // create write command
       val cmdWrite = WriteCommand(item)
       val payload : Array[Byte] = CommandSerializer.encode(cmdWrite)
       tlogCommandsWriter.write_sync(payload)
     }

     var ts : Long = 0L
     var stop : Boolean = false
     var count : Long = 0
     var c : Long = 0

     val time = System.currentTimeMillis()
     Random.setSeed(time)
     // now lets run the real test
     while (!stop) {
       var itemsToRetrive = Random.nextInt(chunkSize) + 1
       val d = tlogCommandsReader.read(ts,itemsToRetrive) match {
         case (times,vec) => {
           // if vector empty check the bug that happend when the reader was initiated before the writer
           for(item <- vec) {
             val payload = item._2
             val cmd = CommandSerializer.decode(payload)
             cmd match {
               case WriteCommand(infoton,trackingID,prevUUID) =>
                 infoton.path should equal ("/command-test/objinfo" + c)
                 c = c + 1
                 assert(true)
               case _ => fail()
             }
           }
           count = count + vec.size
           if ( count == numberOfWrites)
             stop = true
           ts = times
         }
       }
     }
     tlogCommandsReader.shutdown()
   }


     "write to tlog" should "be successful" in {
     val m = collection.mutable.Map[String, String]()
     for ( i <- 0 until numberOfWrites) {
       val payload : String = "Linear Test %d".format(i)
       m += ( payload -> payload )
       tlogLin.write_sync(payload.getBytes)

     }
     Thread.sleep(1000)
     tlogLinDummy = TLog("payload", "w1", bucketSize = 1L * 1024L * 1024L)
     tlogLinDummy.init()
     Thread.sleep(1000)

     var ts : Long = 0L

     var stop : Boolean = false
     var count : Long = 0

     while (!stop) {
       var itemsToRetrive = Random.nextInt(chunkSize) + 1

       val d = tlogLinDummy.read(ts,itemsToRetrive) match {
         case (times,vec) => {
           for(item <- vec) {
             val payload = new String(item._2)
             // first lets check if payload exists and than we will remove it
             if(!m.contains(payload)) fail(s"m should contain key($payload)") //using scalatest Inspectors is too heavy and slows the build
             m -= payload
           }
           count = count + vec.size

           if ( count == numberOfWrites)
             stop = true
           ts = times
         }
       }
     }

     m.isEmpty should equal (true)
     tlogLinDummy.shutdown()
   }


    "write to tlog parallel" should "be successful" in {

      val m = collection.mutable.Map[String, String]()
      for ( i <- 0 until numberOfWrites) {
        val payload : String = "Par Test %d".format(i)
        m += ( payload -> payload )
      }
      m.par foreach { p => tlogPra.write_sync(p._2.getBytes())}
      Thread.sleep(1000)

      val tlogDummy = TLog("payload_p", "w1", bucketSize = 1L * 1024L * 1024L)
      tlogDummy.init()

      var ts : Long = 0L

      var stop : Boolean = false
      var count : Long = 0

      while (!stop) {
        var itemsToRetrive = Random.nextInt(chunkSize) + 1

        val d = tlogPra.read(ts,itemsToRetrive) match {
          case (times,vec) => {
            for(item <- vec) {
              val payload = new String(item._2)
              // first lets check if payload exists and than we will remove it
              if(!m.contains(payload)) fail(s"m should contain key($payload)") //using scalatest Inspectors is too heavy and slows the build
              m -= payload
            }
            count = count + vec.size
            if ( count == numberOfWrites)
              stop = true
            ts = times
          }
        }

      }
      m.isEmpty should equal (true)
      tlogDummy.shutdown()
    }



   "continuous tlog test" should "be successful" in {
     val tlogPraCont0 : TLog = TLog("payload_p_cnt" ,"w1" , bucketSize = 1L * 1024L * 1024L)
     tlogPraCont0.init()

     val m0 = collection.mutable.Map[String, String]()
     for ( i <- 0 until numberOfWrites) {
       val payload : String = "Par Test Cont 0 %d".format(i)
       m0 += ( payload -> payload )
     }
     m0.par foreach { p => tlogPraCont0.write_sync(p._2.getBytes())}
     Thread.sleep(1000)

     var stop : Boolean = false
     var count : Long = 0

     var ts : Long = 0L

     while (!stop) {
       var itemsToRetrive = Random.nextInt(chunkSize) + 1

       val d = tlogPraCont0.read(ts,itemsToRetrive) match {
         case (times,vec) => {
           for(item <- vec) {
             val payload = new String(item._2)
             // first lets check if payload exists and than we will remove it
             if(!m0.contains(payload)) fail(s"m0 should contain key($payload)") //using scalatest Inspectors is too heavy and slows the build
              m0 -= payload
           }
           count = count + vec.size
           if ( count == numberOfWrites)
             stop = true
           ts = times
         }
       }
     }
     m0.isEmpty should equal (true)
     tlogPraCont0.shutdown()

     Thread.sleep(1000)

     val tlogPraCont1 : TLog = TLog("payload_p_cnt" ,"w1", bucketSize = 1L * 1024L * 1024L)
     tlogPraCont1.init()

     val m1 = collection.mutable.Map[String, String]()
     for ( i <- 0 until numberOfWrites) {
       val payload : String = "Par Test Cont 1 %d".format(i)
       m1 += ( payload -> payload )
     }
     m1.par foreach { p => tlogPraCont1.write_sync(p._2.getBytes())}

     stop = false
     count = 0

     Thread.sleep(1000)

     while (!stop) {
       var itemsToRetrive = Random.nextInt(chunkSize) + 1

       val d = tlogPraCont1.read(ts,itemsToRetrive) match {
         case (times,vec) => {
           for(item <- vec) {
             val payload = new String(item._2)
             if(!m1.contains(payload)) fail(s"m1 should contain key($payload)") //using scalatest Inspectors is too heavy and slows the build
             m1 -= payload
           }
           count = count + vec.size
           if ( count == numberOfWrites)
             stop = true
           ts = times
         }
       }
     }
     m1.isEmpty should equal (true)
     // shutdown tlogs
     tlogPraCont1.shutdown()
   }


   "p tlog read write" should "be successful" in {

     val time = System.currentTimeMillis()
     Random.setSeed(time)

     val tlogWriter : TLog = TLog("payload_p_w_r" ,"w1", bucketSize = 1L * 1024L * 1024L )
     tlogWriter.init()
     val tlogReader = tlogWriter
      // build collection to write
     val m0 = collection.mutable.Map[String, String]()
     for ( i <- 0 until numberOfWrites) {
       val payload : String = "Par Read&Write Test %05d".format(i)
       m0 += ( payload -> payload )
     }

     val tWrite = Future {
      m0.par foreach { p => tlogWriter.write_sync(p._2.getBytes())}
     }

      var stop : Boolean = false
      var count : Long = 0

      var ts : Long = 0L

      while (!stop) {
        val itemsToRetrive = Random.nextInt(chunkSize) + 1
        val d = tlogReader.read(ts,itemsToRetrive) match {
          case (times,vec) => {
            for(item <- vec) {
              val payload = new String(item._2)
              // first lets check if payload exists and than we will remove it
              if(!m0.contains(payload)) fail(s"m0 should contain key($payload)") //using scalatest Inspectors is too heavy and slows the build
              m0 -= payload
            }
            count = count + vec.size
            if ( count == numberOfWrites)
              stop = true
            ts = times
          }
        }
      }
     m0.isEmpty should equal (true)

     tlogWriter.shutdown()
     tlogReader.shutdown()

   }
  "tlog position check" should "be successful" in {
    val tlogWriter : TLog = TLog("position" ,"w1" , System.getProperty("cmwell.home") , false, 1L * 1024L * 1024L )
    val tlogReader : TLog = TLog("position" ,"w1" , System.getProperty("cmwell.home") , false, 1L * 1024L * 1024L )

    val tlogPosition : TLog = TLog("position" ,"w1" , System.getProperty("cmwell.home") , true, 1L * 1024L * 1024L)
    tlogPosition.init()

    tlogWriter.init()
    tlogReader.init()

    val m0 = collection.mutable.Map[String, String]()

    for ( i <- 0 until numberOfWrites * 10) {
      val payload : String = "tlogPosition Read&Write Test %05d".format(i)
      m0 += ( payload -> payload )
    }
    // lets make the run the operation during reading
    val tWrite = Future {
      m0.par foreach { p => tlogWriter.write_sync(p._2.getBytes())}
    }

    Thread.sleep(2 * 1000)

    var stop : Boolean = false
    var count : Long = 0
    var t1 : Long = System.currentTimeMillis()

    var ts : Long = 0L

    while (!stop) {
      val itemsToRetrive = Random.nextInt(chunkSize) + 1
      val d = tlogReader.read(ts,itemsToRetrive) match {
        case (times,vec) => {

          for(item <- vec) {
            val payload = new String(item._2)
            // first lets check if payload exists and than we will remove it
            if(!m0.contains(payload)) fail(s"m0 should contain key($payload)") //using scalatest Inspectors is too heavy and slows the build
            m0 -= payload
          }
          count = count + vec.size

          if ( count == numberOfWrites * 10)
            stop = true
          ts = times
          // lets check that ts is >= position
          if ( System.currentTimeMillis() - t1 > 1000 * 10 ) {
            t1 = System.currentTimeMillis()
            // we are reading a head can not be
            tlogPosition.size should be >= ts
          }
        }
      }
    }
    m0.isEmpty should equal (true)
    tlogPosition.shutdown()
    tlogWriter.shutdown()
    tlogReader.shutdown()
  }
}
