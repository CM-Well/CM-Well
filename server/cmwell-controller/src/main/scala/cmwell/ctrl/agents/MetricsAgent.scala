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
//package cmwell.ctrl.agents
//
//import java.lang.instrument.Instrumentation
//import java.util.concurrent.Executors
//import javax.management.openmbean.CompositeData
//import javax.management.{Notification, NotificationListener}
//
//import akka.actor.Actor.Receive
//import akka.actor.{ActorRef, Actor, ActorLogging}
//import cmwell.ctrl.client.CtrlClient
//import cmwell.ctrl.hc.{HealthActor, GcStats, HeakupLatency}
//import k.grid.Grid
//import scala.concurrent.duration._
//import java.lang.management._
//import scala.collection.JavaConversions._
//
//import cmwell.ctrl.config.Config._
//import org.HdrHistogram._
//
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.language.postfixOps
//
///**
// * Created by michael on 12/16/14.
// */
//
//
//case object GetMemoryUsage
//case class MemoryUsage(heapSize : Long, maxHeapSize : Long, freeHeapSize : Long)
//
//case object GetGcStats
//
//case class RecordLatencyValue(value : Long)
//class HeakupMeasure(ref : ActorRef) {
//  private def currentTimestamp : Long = System.currentTimeMillis
//  private val scheduler = Executors.newScheduledThreadPool(1)
//
//  @volatile
//  private[this] var timestamp = currentTimestamp
//
//  class LatencyChecker extends Runnable {
//    override def run(): Unit = {
//      val now = currentTimestamp
//
//      val latency = Math.abs(now - timestamp - heakupSampleInterval)
//      ref ! RecordLatencyValue(latency)
//
//      timestamp = currentTimestamp
//    }
//  }
//
//  private val s = scheduler.scheduleAtFixedRate(new LatencyChecker, 0, heakupSampleInterval, MILLISECONDS)
//}
//
//class MetricsActor extends Actor with ActorLogging {
//
//
//  case object UpdateGcStats
//  case object UpdateHeakupLatency
//
//
//
//  private var previousTimeInGc = 0L
//  private var previousAmountOfGc = 0L
//
//  private var currentTimeInGc = 0L
//  private var currentAmountOfGc = 0L
//
//  private val highestTrackableValue = 3600L * 1000 * 1000
//  private val histogram = new Histogram(highestTrackableValue, 3)
//
//  lazy val pid = ManagementFactory.getRuntimeMXBean().getName().split('@')(0).toInt
//
//  private def updateGCStats = {
//    var totalGarbageCollections : Long = 0
//    var garbageCollectionTime : Long = 0
//
//    val l = ManagementFactory.getGarbageCollectorMXBeans().toList
//    for(gc <- l) {
//      var count = gc.getCollectionCount()
//      if(count >= 0) {
//        totalGarbageCollections += count
//      }
//
//      var time : Long = gc.getCollectionTime()
//
//      if(time >= 0) {
//        garbageCollectionTime += time
//      }
//    }
//
//    /*println("Total Garbage Collections: " + totalGarbageCollections)
//    println("Total Garbage Collection Time (ms): " + garbageCollectionTime)*/
//
//    previousAmountOfGc = currentAmountOfGc
//    currentAmountOfGc = totalGarbageCollections
//
//    previousTimeInGc = currentTimeInGc
//    currentTimeInGc = garbageCollectionTime
//    //log.info("sending gc stats to health actor")
//    HealthActor.ref ! GcStats(listenAddress , pid, roles, currentTimeInGc - previousTimeInGc, currentAmountOfGc - previousAmountOfGc, gcSampleInterval)
//  }
//
//  var hm = new HeakupMeasure(self)
//
//  val system = Grid.system
//  system.scheduler.schedule(gcSampleInterval milli,gcSampleInterval milli, self, UpdateGcStats)
//  system.scheduler.schedule(heakupReportInterval milli,heakupReportInterval milli, self, UpdateHeakupLatency)
//
//
//
//  override def receive: Receive = {
//    case GetMemoryUsage =>
//      val runtime = Runtime.getRuntime()
//      sender ! MemoryUsage(runtime.totalMemory(),runtime.maxMemory(), runtime.freeMemory())
//
//    case UpdateGcStats => updateGCStats
//    case RecordLatencyValue(v) => histogram.recordValue(v)
//    case UpdateHeakupLatency =>
//      HealthActor.ref ! HeakupLatency(listenAddress, pid, roles, histogram.getValueAtPercentile(25.0),
//        histogram.getValueAtPercentile(50.0),
//        histogram.getValueAtPercentile(75.0),
//        histogram.getValueAtPercentile(90.0),
//        histogram.getValueAtPercentile(99.0),
//        histogram.getValueAtPercentile(99.5),
//        histogram.getMaxValue)
////    case GetGcStats => sender ! GcStats(currentTimeInGc - previousTimeInGc, currentAmountOfGc - previousAmountOfGc)
//  }
//}
//
//object MetricsAgent {
//  def agentmain(agentArgs : String, inst : Instrumentation) : Unit = {
//
//  }
//
//  def premain(agentArgs : String, inst : Instrumentation) : Unit = {
////    CtrlClient.join(clusterName, listenAddress, 0, seedNodes, roles)
////    Grid.create(classOf[MetricsActor],"metricsActor")
//  }
//}
