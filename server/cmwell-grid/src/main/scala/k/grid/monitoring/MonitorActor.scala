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


package k.grid.monitoring

import java.io.{File, FileInputStream, InputStream}

import akka.actor.{Actor, Cancellable}
import akka.actor.Actor.Receive
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.LazyLogging
import k.grid.dmap.impl.inmem.InMemDMap
import k.grid.webapi.JsonApi
import k.grid.{Formatters, Grid}
import org.slf4j.LoggerFactory
//import spray.can.Http
//import spray.http.HttpHeaders.`Content-Type`
//import spray.http.HttpHeaders._
//import spray.http.ContentTypes._
//import spray.http.HttpMethods._
//import spray.http._
import akka.pattern.pipe

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.duration
import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global
/**
 * Created by michael on 7/6/15.
 */

object MonitorActor {
  val name = "MonitorActor"
}



/*
  LoggerFactory.getLogger("ROOT").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.DEBUG)
  LoggerFactory.getLogger("ROOT").asInstanceOf[ch.qos.logback.classic.Logger].getLevel
 */

object SetNodeLogLevel {
  val lvlMappings = Map(
    "OFF" -> Level.OFF,
    "ERROR" -> Level.ERROR,
    "WARN" -> Level.WARN,
    "INFO" -> Level.INFO,
    "DEBUG" -> Level.DEBUG,
    "TRACE" -> Level.TRACE,
    "ALL" -> Level.ALL
  )

  def levelTranslator(lvl : String) : Option[Level] = {
    lvlMappings.get(lvl.toUpperCase)
  }
}

case object GetNodeLogLevel
case class NodeLogLevel(lvl : String)
case class SetNodeLogLevel(level : Level, levelDuration : Option[Int] = Some(10))

class MonitorActor extends Actor with LazyLogging {
  private[this] var originalLogLevel : Level = _
  private val editableLogger = "ROOT"
  private[this] var scheduledLogLevelReset : Cancellable = _

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    //FIXME: what if logger is not logback? use jmx or write more defensive code
    originalLogLevel = {
      val l = LoggerFactory.getLogger(editableLogger)
      val f = l.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
      l.info("logger is loaded from: " + f)
      l.asInstanceOf[ch.qos.logback.classic.Logger].getLevel
    }
  }

  override def receive: Receive = {
//    case _: Http.Connected => sender ! Http.Register(self)
//
//    case r@HttpRequest(GET, Uri.Path("/monitor/settings"), _, _, _) =>
//      val s = InMemDMap.sm.map(tpl => s"${tpl._1} -> ${tpl._2}").mkString("\n")
//      sender ! HttpResponse(entity = s)
//
//    case r@HttpRequest(GET, Uri.Path("/monitor/members"), _, _, _) =>
//      Grid.getRunningJvmsInfo.foreach {
//        jvmsInfo => sender ! HttpResponse(entity = Formatters.membersFormatter(jvmsInfo))
//      }
//
//    case r@HttpRequest(GET, Uri.Path("/monitor/singletons"), _, _, _) =>
//      sender ! HttpResponse(entity = Formatters.singletonsFormatter(Grid.getSingletonsInfo))
//
//    case GetSingletons => sender ! Grid.getSingletons
//
//    case r@HttpRequest(GET, Uri.Path("/monitor/actors"), _, _, _) =>
//      val senderVal = sender
//      val response = Grid.allActors
//      response.map{x => senderVal ! HttpResponse(entity = Formatters.activeActorsFormatter(x))}
//
//    case r@HttpRequest(GET, Uri.Path("/test/perm"), _, _, _) =>
//      ProcKiller.add
//      sender ! HttpResponse(entity = "Done!")
//
//    case r@HttpRequest(GET, Uri.Path("/test/temp"), _, _, _) =>
//      ProcKiller.genString
//      sender ! HttpResponse(entity = "Done!")
//
//    case r@HttpRequest(GET, Uri.Path("/api/json/jvms"), _, _, _) =>
//      sender ! HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), JsonApi.getJvmsJson ))
//
//
//    case r@HttpRequest(GET, Uri.Path("/api/json/mem"), _, _, _) =>
//      val senderVal = sender
//      JsonApi.getMemJson.foreach {
//        mj =>
//          senderVal ! HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), mj ))
//      }
//
//
//    case r@HttpRequest(GET, Uri.Path("/api/js/d3.min.js"), _, _, _) =>
//      val stream : InputStream = getClass.getResourceAsStream("/js/d3.min.js")
//      val lines = scala.io.Source.fromInputStream( stream ).getLines.mkString("\n")
//      sender ! HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/javascript`), lines ))
//
//    case r@HttpRequest(GET, Uri.Path("/api/js/underscore-min.js"), _, _, _) =>
//      val stream : InputStream = getClass.getResourceAsStream("/js/underscore-min.js")
//      val lines = scala.io.Source.fromInputStream( stream ).getLines.mkString("\n")
//      sender ! HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/javascript`), lines ))
//
//
//    case r@HttpRequest(GET, Uri.Path("/api/js/Chart.js"), _, _, _) =>
//      val stream : InputStream = getClass.getResourceAsStream("/js/Chart.js")
//      val lines = scala.io.Source.fromInputStream( stream ).getLines.mkString("\n")
//      sender ! HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/javascript`), lines ))
//      //Chart.min.js
//
//    case r@HttpRequest(GET, Uri.Path("/api/html/jvms"), _, _, _) =>
//      val stream : InputStream = getClass.getResourceAsStream("/html/jvms.html")
//      val lines = scala.io.Source.fromInputStream( stream ).getLines.mkString("\n")
//      sender ! HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`text/html`), lines ))
//
//    case r@HttpRequest(GET, Uri.Path("/api/html/mem"), _, _, _) =>
//      val stream : InputStream = getClass.getResourceAsStream("/html/mem.html")
//
////      val fileName = "/home/michael/cmwell/cmwell-grid/src/main/resources/html/mem.html"
////
////      val stream : InputStream = new FileInputStream(new File(fileName))
//      val lines = scala.io.Source.fromInputStream( stream ).getLines.mkString("\n")
//      sender ! HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`text/html`), lines ))
//

    case PingChildren =>
      MonitorUtil.pingChildren.pipeTo(sender)

    case SetNodeLogLevel(lvl, duration) =>
      if(scheduledLogLevelReset != null) {
        scheduledLogLevelReset.cancel()
        scheduledLogLevelReset = null
      }

      logger.info(s"Setting $editableLogger to log level $lvl")
      duration.foreach { d =>
        logger.info(s"Scheduling $editableLogger to be in level $originalLogLevel in $d minutes")
        scheduledLogLevelReset = context.system.scheduler.scheduleOnce(d.minutes, self, SetNodeLogLevel(originalLogLevel, None))
      }


      LoggerFactory.getLogger(editableLogger).asInstanceOf[ch.qos.logback.classic.Logger].setLevel(lvl)
      //change also the log level of the akka logger
      val akkaLoggerName = "akka"
      LoggerFactory.getLogger(akkaLoggerName) match {
        case akkaLogger: Logger =>
          if (akkaLogger != null)
            akkaLogger.setLevel(lvl)
        case _ =>
      }
    //Grid.getSingletonRef("DcController", Some("DcClient"))
    case GetNodeLogLevel =>
      val lvl = LoggerFactory.getLogger(editableLogger).asInstanceOf[ch.qos.logback.classic.Logger].getLevel
      sender ! NodeLogLevel(lvl.toString)
  }
}
