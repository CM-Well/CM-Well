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
package actions

import java.util.TimeZone

import akka.actor._
import akka.actor.Actor.Receive
import akka.util.Timeout
import cmwell.ctrl.config.Jvms
import cmwell.domain.{FileContent, FileInfoton, SystemFields, VirtualInfoton}
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid
import akka.pattern.{ask, pipe}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import play.api.mvc.{AnyContent, Request, Result}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import cmwell.util.concurrent.successes
import cmwell.util.string.{Base64, dateStringify}
import cmwell.ws.Settings

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

/**
  * Created by michael on 7/14/15.
  */
case object GetRequest
case object GetRequestShort
class RequestMonitor(cmwellRequest: CmwellRequest) extends Actor with LazyLogging {
  implicit val timeout = Timeout(30.seconds)

  val deathWatch = 30.seconds

  context.system.scheduler.scheduleOnce(deathWatch, self, PoisonPill)

  override def receive: Receive = {
    case GetRequest =>
      logger.debug("Got GetRequest")
      sender ! cmwellRequest
    case GetRequestShort =>
      logger.debug("Got GetRequestShort")
      sender ! cmwellRequest.copy(requestBody = cmwellRequest.requestBody.take(100))
  }
}

case class CmwellRequest(requestType: String,
                         path: String,
                         queryString: String,
                         generationTime: Long,
                         requestBody: String)
case object GetRequestActors

object RequestMonitor extends LazyLogging {
  implicit val timeout = Timeout(30.seconds)
  private[this] var requestContainer: ActorRef = _

  def getRequestContainer = requestContainer

  def init: Unit = {
    requestContainer = Grid.create(classOf[RequestsContainer], "RequestsContainer")
  }

  def add(requestType: String, path: String, queryString: String, requestBody: String, now: Long): Unit = {
    requestContainer ! CmwellRequest(requestType, path, queryString, now, requestBody)
  }

  def getAllRequestPaths: Future[Map[String, Iterable[(String, CmwellRequest)]]] = {
    startMonitoring

    val webHosts = Grid.jvms(Jvms.WS)
    logger.info(s"webHosts: $webHosts")
    val futures = webHosts.map { host =>
      logger.info(s"host: $host")
      (Grid.selectActor("RequestsContainer", host) ? GetRequestActors)
        .mapTo[Iterable[(String, CmwellRequest)]]
        .map(it => host -> it)
    }
    successes(futures).map(vec => vec.map(el => el._1.host -> el._2).toMap)
  }

  def requestsInfoton(path: String, dc: String): Future[Option[VirtualInfoton]] = {
    val res = getAllRequestPaths.map { m =>
      logger.info(s"GOT MAP: $m")
      val bd = m.toSeq
        .sortBy(_._1)
        .map { tuple =>
          s"""***${tuple._1}*** <br>
               |
               |""".stripMargin +
            MarkdownTable(
              MarkdownTuple("Details", "Type", "Path", "Query String", "Generation Time"),
              tuple._2.map { c =>
                val jodaTime = dateStringify(new DateTime(c._2.generationTime))

                MarkdownTuple(s"<a href=/monitor/requests/${c._1}>Details</a>",
                              c._2.requestType,
                              c._2.path,
                              c._2.queryString,
                              jodaTime)
              }.toSeq
            ).get
        }
        .mkString("\n\n\n")
      Some(VirtualInfoton(FileInfoton(SystemFields(path, new DateTime(DateTimeZone.UTC), "VirtualInfoton", dc, None, "", "http"),
        None, content = Some(FileContent(bd.getBytes, "text/x-markdown")))))
    }
    res
  }

  def startMonitoring {
    Grid.jvms(Jvms.WS).foreach { host =>
      Grid.selectActor("RequestsContainer", host) ! StartRequestMonitoring
    }
  }

  def stopMonitoring {
    Grid.jvms(Jvms.WS).foreach { host =>
      Grid.selectActor("RequestsContainer", host) ! StopRequestMonitoring
    }
  }

}

case object StartRequestMonitoring
case object StopRequestMonitoring

class RequestsContainer extends Actor with LazyLogging {
  implicit val timeout = Timeout(30.seconds)
  var actors = Map.empty[String, ActorRef]

  private[this] var doMonitoring: Boolean = false
  private[this] var taskCanceller: Option[Cancellable] = None

  override def receive: Actor.Receive = {
    case cmwr: CmwellRequest =>
      if (doMonitoring) {
        logger.info(s"adding CmwellRequest: $cmwr")
        context.actorOf(Props(classOf[RequestMonitor], cmwr))
        logger.info(s"current amount of children: ${context.children.size}")
      }

    case GetRequestActors =>
      logger.info(s"Received GetRequestActors has (${context.children.size}) children")
      val s = sender()
      val x = context.children.map { c =>
        logger.debug(s"path: ${c.path.toSerializationFormatWithAddress(Grid.me)}")
        val pathB64 = Base64.encodeBase64String(c.path.toSerializationFormatWithAddress(Grid.me))
        //logger.info(s"pathB64: $pathB64")
        (c ? GetRequestShort).mapTo[CmwellRequest].map { f =>
          (pathB64, f)
        }
      }

      //val y = context.children.map(c => c.path.toSerializationFormatWithAddress(Grid.me))
      successes(x).foreach { xx =>
        logger.debug(s"Request contents: ${xx.toString}")
        s ! xx
      }

    case StartRequestMonitoring =>
      logger.info("Starting request monitoring")
      doMonitoring = true

      taskCanceller.map(_.cancel())
      taskCanceller = Some(context.system.scheduler.scheduleOnce(5 minutes, self, StopRequestMonitoring))

    case StopRequestMonitoring =>
      logger.info("Stopping request monitoring")
      taskCanceller = None
      doMonitoring = false
  }
}
