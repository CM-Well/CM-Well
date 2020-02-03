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
package k.grid.monitoring

import akka.actor.{Actor, Cancellable}
import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory
import akka.pattern.pipe
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object MonitorActor {
  val name = "MonitorActor"
}

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

  def levelTranslator(lvl: String): Option[Level] = {
    lvlMappings.get(lvl.toUpperCase)
  }
}

case object GetNodeLogLevel
case class NodeLogLevel(lvl: String)
case class SetNodeLogLevel(level: Level, levelDuration: Option[Int] = Some(10))

class MonitorActor extends Actor with LazyLogging {
  private[this] var originalLogLevel: Level = _
  private val editableLogger = "ROOT"
  private[this] var scheduledLogLevelReset: Cancellable = _

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
    case PingChildren =>
      MonitorUtil.pingChildren.pipeTo(sender)

    case SetNodeLogLevel(lvl, duration) =>
      if (scheduledLogLevelReset != null) {
        scheduledLogLevelReset.cancel()
        scheduledLogLevelReset = null
      }

      logger.info(s"Setting $editableLogger to log level $lvl")
      duration.foreach { d =>
        logger.info(s"Scheduling $editableLogger to be in level $originalLogLevel in $d minutes")
        scheduledLogLevelReset =
          context.system.scheduler.scheduleOnce(d.minutes, self, SetNodeLogLevel(originalLogLevel, None))
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
    case GetNodeLogLevel =>
      val lvl = LoggerFactory.getLogger(editableLogger).asInstanceOf[ch.qos.logback.classic.Logger].getLevel
      sender ! NodeLogLevel(lvl.toString)
  }
}
