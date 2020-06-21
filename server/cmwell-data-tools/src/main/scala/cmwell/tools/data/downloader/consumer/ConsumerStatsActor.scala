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
package cmwell.tools.data.downloader.consumer

import akka.actor.{Actor, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy}
import akka.actor.Actor.Receive
import akka.actor.SupervisorStrategy.Resume
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import cmwell.tools.data.utils.ArgsManipulations.{formatHost, HttpAddress}
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.text.Tokens
import akka.pattern._
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.logging.DataToolsLogging

import scala.concurrent.duration._
import scala.util.Success

/**
  * Created by matan on 20/3/17.
  */
/*
class ConsumerStatsActor(baseUrl: String,
                         initToken: String,
                         params: String = "",
                         override val label: Option[String] = None) extends Actor with DataToolsLogging {

  case object Status
  case object Reset

  var counter = 0L
  var expectedNumInfotons = 0L
  var currentToken: String = initToken
  var cancellableStatus: Cancellable = _
  var numInfotonsFromPreviousTokens = 0L
  var requestedNumRecordsAtStart = false

  implicit val system = context.system
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher


  override def postRestart(reason: Throwable): Unit = {
    logger.info(s"I finished restarting", reason)
  }


  override def postStop(): Unit = {
    if (cancellableStatus != null) cancellableStatus.cancel()
  }

  override val receive: Receive = receiveBeforeNewData

  def receiveBeforeNewData: Receive = {
    case ConsumeEvent if requestedNumRecordsAtStart =>
      counter += 1

    case ConsumeEvent =>
      counter += 1
      logger.info(s"started working on init-token $initToken")
      requestedNumRecordsAtStart = true
      getNumRecords().map(NumRecordsAtStart.apply) pipeTo self

    case NumRecordsAtStart(num) =>
      expectedNumInfotons = num
      cancellableStatus = context.system.scheduler.schedule(1.second, 5.seconds, self, Status)

    case NewToken(t) =>
      currentToken = t
      logger.info(s"received new token: $t")
      numInfotonsFromPreviousTokens = counter // counter checkpoint

    case EndStreamEvent =>
      getNumRecords().map(NumRecordsAtEnd.apply) pipeTo self

    case NumRecordsAtEnd(num) =>
      logger.info(s"received $counter/$expectedNumInfotons vs. $num infotons (end-time) current-token=$currentToken init-token $initToken")
      logger.info(s"finished working, going to kill myself init-token $initToken")
      context stop self

    case Status =>
      logger.info(s"received $counter/$expectedNumInfotons infotons, current-token=$currentToken, init-token $initToken")

    case Reset =>
      logger.info("reset statistics")
      logger.info(s"old counter = $counter")
      counter = numInfotonsFromPreviousTokens
      logger.info(s"new counter = $counter")

    case x =>
      logger.warn(s"unexpected message: $x")
  }

//  def receiveAfterNewData: Receive = {
//    case NumRecordsAtStart(num) =>
//      expectedNumInfotons = num
//      cancellableStatus = context.system.scheduler.schedule(1.second, 5.seconds, self, Status)
//    case NumRecordsAtEnd(num) =>
//      logger.info(s"received $counter/$expectedNumInfotons vs. $num infotons (end-time) current-token=$currentToken init-token $initToken")
//      logger.info(s"finished working, going to kill myself init-token $initToken")
//      context stop self
//    case ConsumeEvent =>
//      counter += 1
//    case EndStreamEvent =>
//      getNumRecords().map(NumRecordsAtEnd.apply) pipeTo self
//    case Status =>
//      logger.info(s"received $counter/$expectedNumInfotons infotons, current-token=$currentToken, init-token $initToken")
//    case NewToken(t) =>
//      currentToken = t
//      logger.info(s"received new token: $t")
//      numInfotonsFromPreviousTokens = counter // counter checkpoint
//    case Reset =>
//      logger.info("reset statistics")
//      logger.info(s"old counter = $counter")
//      counter = numInfotonsFromPreviousTokens
//      logger.info(s"new counter = $counter")
//    case x =>
//      logger.warn(s"unexpected message on afterNewData: $x")
//  }

  def getNumRecords() = {
    val decodedToken = Tokens.decompress(initToken).split('|')
    val indexTime = decodedToken(0).toLong
    val qp = if (decodedToken.size < 3) "" else s"&qp=${decodedToken.last}&indexTime=$indexTime"
    val path = decodedToken(1)

    // http query parameters which should be always present
    val httpParams = Map(
      "op" -> "search",
      "qp" -> decodedToken.last,
      "indexTime" -> indexTime.toString,
      "format" -> "json",
      "length" -> "1",
      "pretty" -> "")

    // http query parameters which are optional (i.e., API Garden)
    val paramsMap = params.split("&")
      .map(_.split("="))
      .collect {
        case Array(k, v) => (k, v)
        case Array(k) if k.nonEmpty => (k, "")
      }.toMap

    val req = HttpRequest(uri = Uri(s"${formatHost(baseUrl)}$path").withQuery(Query(httpParams ++ paramsMap)))

    logger.info(s"send stats request: ${req.uri}")

    val HttpAddress(protocol, host, port, uriPrefix) = ArgsManipulations.extractBaseUrl(baseUrl)
    val conn = HttpConnections.cachedHostConnectionPool[Option[_]](host, port, protocol)

    Source.single(req -> None)
      .via(conn)
      .mapAsync(1) {
        case (Success(HttpResponse(s, _, e, _)), _) =>
          e.withoutSizeLimit().dataBytes
            .via(lineSeparatorFrame)
            .filter(_.utf8String.trim contains "\"total\" : ")
            .map(_.utf8String)
            .map(_.split(":")(1).init.tail.toLong)
            .runWith(Sink.head)
      }
      .runWith(Sink.head)
//    Http().singleRequest(req).flatMap { case HttpResponse(s, _, e, _) =>
//      e.withoutSizeLimit().dataBytes
//        .via(lineSeparatorFrame)
//        .filter(_.utf8String.trim contains "\"total\" : ")
//        .map(_.utf8String)
//        .map(_.split(":")(1).init.tail.toLong)
//        .runWith(Sink.head)
//    }
  }
}
 */

case object ConsumeEvent
case object EndStreamEvent
case class NumRecordsAtEnd(num: Long)
case class NumRecordsAtStart(num: Long)
case class NewToken(token: String)
