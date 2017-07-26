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


package cmwell.tools.data.downloader.consumer

import akka.actor.{Actor, ActorSystem}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.pattern._
import akka.stream.scaladsl._
import akka.stream._
import cmwell.tools.data.downloader.consumer.Downloader._
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.{HttpAddress, formatHost}
import cmwell.tools.data.utils.akka.HeaderOps.getPosition
import cmwell.tools.data.utils.akka.{DataToolsConfig, HttpConnections, lineSeparatorFrame}
import cmwell.tools.data.utils.logging._
import cmwell.tools.data.utils.text.Tokens

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

object BufferFillerActor {
  case object Status
  case class FinishedToken(nextToken: Option[Token])
  case class NewData(data: Option[(Token,TsvData)])
  case class InitToken(token: Token)
  case object GetData
  case object RequestExpectedNumInfotons
  case class ResponseExpectedNumInfotons(num: Long, token: Token)
}

class BufferFillerActor(threshold: Int,
                        initToken: Future[Token],
                        baseUrl: String,
                        params: String = "",
                        isBulk: Boolean = false,
                        updateFreq: Option[FiniteDuration] = None,
                        override val label: Option[String] = None) extends Actor with DataToolsLogging with DataToolsConfig {
  import BufferFillerActor._

  implicit val ec: ExecutionContext = context.dispatcher
  implicit val system: ActorSystem  = context.system
  implicit val mat: Materializer    = ActorMaterializer()
  implicit val labelId = label.map(LabelId.apply)

  val receivedUuids = mutable.Set.empty[Uuid]
  val uuidsFromCurrentToken = mutable.Set.empty[Uuid]
  private var currToken: Token = _
  private var currConsumeState: ConsumeState = SuccessState(0)
  private var currKillSwitch: Option[KillSwitch] = None
  private val buf: mutable.Queue[Option[(Token, TsvData)]] = mutable.Queue()
  private var tsvCounter = 0L

  val retryTimeout: FiniteDuration = {
    val timeoutDuration = Duration(config.getString("cmwell.downloader.consumer.http-retry-timeout")).toCoarsest
    FiniteDuration( timeoutDuration.length, timeoutDuration.unit )
  }

  private val HttpAddress(protocol, host, port, _) = ArgsManipulations.extractBaseUrl(baseUrl)
  private val conn = HttpConnections.newHostConnectionPool[Option[_]](host, port, protocol)

  override def preStart(): Unit = {
    initToken.map(InitToken.apply) pipeTo self
  }

  override def postRestart(reason: Throwable): Unit = {
    logger.error("I'm in restart", reason)
  }

  override def postStop(): Unit = {
    logger.info("I'm in post stop")
    currKillSwitch.foreach(_.shutdown())
  }

  override def receive: Receive = {
    case InitToken(token) =>
      currToken = token
      self ! RequestExpectedNumInfotons
      self ! Status
    case FinishedToken(nextToken) =>
      logger.info(s"received $tsvCounter uuids from token $currToken")

      receivedUuids  --= uuidsFromCurrentToken
      tsvCounter = 0L

      nextToken match {
        case Some(token) =>
          currToken = token
          self ! RequestExpectedNumInfotons
          self ! Status

        case None if updateFreq.nonEmpty =>
          logger.info("no more data is available, will check again in {}", updateFreq.get)
          context.system.scheduler.scheduleOnce(updateFreq.get, self, Status)

        case None =>
          logger.info("no more data is available, will not check again")
      }

    case Status if buf.size < threshold =>
        sendNextChunkRequest(currToken).map(FinishedToken.apply) pipeTo self

    case Status =>
      context.system.scheduler.scheduleOnce(1.seconds, self, Status)

    case NewData(data) =>
      buf += data
      tsvCounter += 1

    case GetData if buf.nonEmpty =>
      sender ! buf.dequeue()

    case GetData =>
      // do nothing since there are no elements in buffer
//      context.system.scheduler.scheduleOnce(1.second, self, GetData)(implicitly[ExecutionContext], sender = sender())

    case RequestExpectedNumInfotons =>
      val token = currToken
      getNumRecords(token).map(numRecords => ResponseExpectedNumInfotons(numRecords, token)) pipeTo self

    case ResponseExpectedNumInfotons(expected, token) =>
      logger.info("expect {} infotons from token {}", expected.toString, token)

    case x =>
      logger.error(s"unexpected message: $x")
  }

  /**
    * Sends request of next data chunk and fills the given buffer
    * @param token cm-well position token to consume its data
    * @return optional next token value, otherwise None when there is no data left to be consumed
    */
  def sendNextChunkRequest(token: String): Future[Option[String]] = {

    /**
      * Creates http request for consuming data
      * @param token position token to be consumed
      * @return HTTP request for consuming data
      */
    def createRequestFromToken(token: String) = {
      // create HTTP request from token
      val paramsValue = if (params.isEmpty) "" else s"&$params"

      val (consumeHandler, slowBulk) = currConsumeState match {
        case SuccessState(_) =>
          val consumeHandler = if (isBulk) "_bulk-consume" else "_consume"
          (consumeHandler, "")
        case LightFailure(_, _) =>
          val consumeHandler = if (isBulk) "_bulk-consume" else "_consume"
          (consumeHandler, "&slow-bulk")
        case HeavyFailure(_) =>
          ("_consume", "&slow-bulk")
      }

      val uri = s"${formatHost(baseUrl)}/$consumeHandler?position=$token&format=tsv$paramsValue$slowBulk"
      logger.debug("send HTTP request: {}", uri)

      HttpRequest(uri = uri)
    }

    //          consumerStatsActor ! NewToken(token)

    uuidsFromCurrentToken.clear()

    val source: Source[Token, (Future[Option[Token]], UniqueKillSwitch)] = Source.single(token)
      .map(createRequestFromToken)
      .map(_ -> None)
      .via(conn)
      .map {
        case (Success(HttpResponse(s, h , e, _)), _) if s == StatusCodes.TooManyRequests =>
          e.discardBytes()

          logger.error(s"HTTP 429: too many requests token=$token")
          None -> Source.failed(new Exception("too many requests"))

        case (Success(HttpResponse(s, h , e, _)), _) if s == StatusCodes.NoContent =>
          e.discardBytes()

//          consumerStatsActor ! EndStreamEvent // also kills the previous actor
//          consumerStatsActor = system.actorOf(Props(new ConsumerStatsActor(baseUrl, initToken, params)))

          if (updateFreq.isEmpty) self ! NewData(None)

          None -> Source.empty
        case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.OK || s == StatusCodes.PartialContent =>
          val nextToken = getPosition(h) match {
            case Some(pos) => pos.value
            case None      => throw new RuntimeException("no position supplied")
          }

          // store expected num infotons from stream
//          expectedNumInfotons = getNumInfotonsValue(h) match {
//            case "NA" => None
//            case x => Some(x.toLong)
//          }

//          logger.info(s"expected num uuids = ${expectedNumInfotons.getOrElse("None")} token=$token")

          val dataSource: Source[(Token, TsvData), Any] = e.withoutSizeLimit().dataBytes
            .via(lineSeparatorFrame)
            .map(extractTsv)
            .map(token -> _)

          Some(nextToken) -> dataSource
        case x =>
          logger.error(s"unexpected message: $x")
          ???
      }
      .alsoToMat(Sink.last)((_,element) => element.map { case (nextToken, _) => nextToken})
      .map { case (_, dataSource) => dataSource}
      .flatMapConcat(identity)
      .map { case (token, tsv) =>
        // if uuid was not emitted before, write it to buffer
        if (receivedUuids.add(tsv.uuid)) {
          self ! NewData(Some((token, tsv)))
        }

        uuidsFromCurrentToken.add(tsv.uuid)
        token
      }
      .viaMat(KillSwitches.single)(Keep.both)



    val (result, killSwitch) = source.toMat(Sink.ignore) { case ((token, killSwitch), done) =>
      done.flatMap(_ => token) -> killSwitch
    }.run()

    currKillSwitch = Some(killSwitch)

//    result
//      .map { case nextToken =>
//        // validate that all uuids were received, if not - fail the future
////        if (expectedNumInfotons.isDefined && receivedUuids.size != expectedNumInfotons.getOrElse(0)) {
////          logger.error(s"not all infotons were consumed (${receivedUuids.size}/${expectedNumInfotons.get}) from token $token")
////        }
////        expectedNumInfotons = None
//        nextToken
//      }

    result.onSuccess{ case _ =>
      // get point in time of token
      val decoded = Try(new org.joda.time.LocalDateTime(Tokens.decompress(token).takeWhile(_ != '|').toLong))
      logger.debug(s"successfully consumed token: $token point in time: ${decoded.getOrElse("")} buffer-size: ${buf.size}")
      currConsumeState = ConsumeStateHandler.nextSuccess(currConsumeState)
    }

    // resilience to failures
    result.recoverWith{ case err =>
//      consumerStatsActor ! Reset
      currConsumeState = ConsumeStateHandler.nextFailure(currConsumeState)
      logger.info(s"error: ${err.getMessage} consumer will perform retry in $retryTimeout, token=$token", err)
      after(retryTimeout, context.system.scheduler)(sendNextChunkRequest(token)) }
  }

  def getNumRecords(token: Token): Future[Long] = {
    val decodedToken = Tokens.decompress(token).split('|')
    val indexTime = decodedToken(0).toLong
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
  }
}