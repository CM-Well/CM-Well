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


//package cmwell.ctrl.checkers
//
//import java.io.File
//import java.nio.ByteBuffer
//import java.nio.file.{Files, Paths}
//import java.util.concurrent.atomic.AtomicLong
//
//import cmwell.ctrl.config.{Config, Jvms}
//import com.typesafe.scalalogging.LazyLogging
//import k.grid.{Grid, GridJvm}
//
//import scala.concurrent.Future
//import scala.concurrent.duration._
//import akka.pattern.ask
//import akka.util.Timeout
//import cmwell.ctrl.config.Config._
//import cmwell.ctrl.utils.ProcUtil
//import k.grid.dmap.api.SettingsLong
//import k.grid.dmap.impl.persistent.PersistentDMap
//
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.util.Try
//
///**
//  * Created by michael on 7/31/16.
//  */
//
//case object GetLastWroteIndexCommandsOffset
//case class LastWroteIndexCommandsOffset(offset:Long)
//
//case class BgStateReporter(topicName : String, partition : Int) {
//
//  var lastReported = 0L
//  var l : AtomicLong = new AtomicLong()
//
//  def set(longVal: Long) = l.set(longVal)
//
//
//  if(Grid.system != null)
//    Grid.system.scheduler.schedule(0.seconds, 5.seconds) {
//      val valueToReport = l.get()
//      if(lastReported != valueToReport) {
//        PersistentDMap.set(s"cmwell.bg.write.$topicName.$partition", SettingsLong(valueToReport))
//        lastReported = valueToReport
//      }
//    }
//}
//
//object BgStateReporter {
//  var m = Map.empty[(String, Int), BgStateReporter]
//  def report(topicName : String, partition : Int, value : Long): Unit = {
//    m.get(topicName, partition) match {
//      case Some(bsr@BgStateReporter(_,_)) =>
//        bsr.set(value)
//      case None =>
//        val bsr = BgStateReporter(topicName, partition)
//        bsr.set(value)
//        m = m.updated((topicName, partition), bsr)
//    }
//  }
//}
//
//object BgChecker extends Checker with LazyLogging {
//  implicit val timeout = Timeout(3 seconds)
//  import collection.JavaConverters._
//  lazy val persistPartition = getPartition("persist_topic")
//  lazy val indexPartition = getPartition("index_topic")
//
//  case class BgData(wsWrite : Long, wsRead : Long, idxWrite : Long, idxRead : Long)
//
//
//  def getPartition(name : String) : Int = {
//    val location = Config.cmwellHome + "/app/bg/"
//    val dir = new File(location)
//    dir.listFiles().toVector.find(_.getName.contains(name)) match {
//      case Some(f) =>
//        val name = f.getName
//        Try(name.split("-|\\.")(1).toInt).getOrElse(-1)
//      case None => -1
//    }
//  }
//
//  def getReadPosition(name : String) : Long = {
//    val location = Config.cmwellHome + "/app/bg/"
//    val dir = new File(location)
//    dir.listFiles().toVector.find(_.getName.contains(name)) match {
//      case Some(f) =>
//        val fullPath = f.getAbsolutePath
//        val readBuffer = ByteBuffer.allocate(8)
//        val readChnl = Files.newByteChannel(Paths.get(fullPath))
//        readChnl.read(readBuffer)
//        readChnl.close()
//        readBuffer.flip()
//        readBuffer.getLong - 1L
//      case None =>
//        logger.error(s"Couldn't find the topic $name read position file in $location.")
//        0L
//    }
//
//
//
//
//  }
//
//  def getWsWritePosition : Future[Long] = {
//    //Grid.selectActor("")
//    (Grid.selectActor("WebWriteMonitor", GridJvm(Jvms.WS)) ? GetLastWroteIndexCommandsOffset).mapTo[LastWroteIndexCommandsOffset].map(_.offset)
//  }
//
//  def getIndexerWritePosition : Future[Long] = {
//    (Grid.selectActor("CMWellBGActor", GridJvm(Jvms.BG)) ? GetLastWroteIndexCommandsOffset).mapTo[LastWroteIndexCommandsOffset].map(_.offset)
//  }
//
//  def getBgData : Future[BgData] = {
//
////
////    for {
////      wsWritePos <- getWsWritePosition
////      idxWritePos <- getIndexerWritePosition
////    } yield {
////      val wsReadPos = getReadPosition("persist_topic")
////      val idxReadPos = getReadPosition("index_topic")
////
////      BgData(wsWritePos, wsReadPos, idxWritePos, idxReadPos)
////    }
//
//    val wsReadPos = getReadPosition("persist_topic")
//    val idxReadPos = getReadPosition("index_topic")
//
//    val wsWritePos = PersistentDMap.get(s"cmwell.bg.write.persist_topic.$persistPartition").flatMap(_.as[SettingsLong]).map(_.lng).getOrElse(0L)
//    val idxWritePos = PersistentDMap.get(s"cmwell.bg.write.index_topic.$indexPartition").flatMap(_.as[SettingsLong]).map(_.lng).getOrElse(0L)
//
//    Future.successful(BgData(wsWritePos, wsReadPos, idxWritePos, idxReadPos))
//  }
//
//  override def check: Future[ComponentState] = {
//
//    if(ProcUtil.checkIfProcessRun("bg") > 0) {
//      val lastState = getLastStates(1).headOption
//      val newState = lastState match {
//        case Some(cs : BatchState) =>
//          getBgData.map {
//            bgData =>
//              if(cs.isIndexing(bgData.wsWrite , bgData.wsRead , bgData.idxWrite , bgData.idxRead))
//                BatchOk(bgData.wsWrite , bgData.wsRead , bgData.idxWrite , bgData.idxRead, (bgData.wsRead - cs.impLocation) / (batchSampleInterval / 1000), (bgData.idxRead - cs.indexerLocation) / (batchSampleInterval / 1000))
//              else
//                BatchNotIndexing(bgData.wsWrite , bgData.wsRead , bgData.idxWrite , bgData.idxRead)
//          }
//
//        case None => getBgData.map(bgData => BatchOk(bgData.wsWrite , bgData.wsRead , bgData.idxWrite , bgData.idxRead))
//        case _ =>
//          logger.error("Wrong ComponentStatus is in BatchStates!")
//          getBgData.map(bgData => BatchOk(bgData.wsWrite , bgData.wsRead , bgData.idxWrite , bgData.idxRead))
//      }
//      newState
//    } else{
//      getBgData.map {
//        bgData => BatchDown(bgData.wsWrite , bgData.wsRead , bgData.idxWrite , bgData.idxRead)
//      }
//    }
//  }
//}
