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
import akka.http.scaladsl.model._
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl._
import cmwell.tools.data.downloader.consumer.Downloader._
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.{HttpAddress, formatHost}
import cmwell.tools.data.utils.akka.HeaderOps._
import cmwell.tools.data.utils.akka.{DataToolsConfig, HttpConnections, lineSeparatorFrame}
import cmwell.tools.data.utils.logging._
import cmwell.tools.data.utils.text.Tokens

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object BufferFillerActor {
  case object Status
  case class FinishedToken(nextToken: Option[Token])
  case class NewData(data: Option[(Token,TsvData)])
  case class InitToken(token: Token)
  case object GetData
  case class HttpResponseSuccess(token: Token)
  case class HttpResponseFailure(token: Token, err: Throwable)
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
  private var lastBulkConsumeToHeader: Option[String] = None

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
      self ! Status
    case FinishedToken(nextToken) =>
      logger.info(s"received $tsvCounter uuids from token $currToken")

      receivedUuids  --= uuidsFromCurrentToken
      tsvCounter = 0L
      lastBulkConsumeToHeader = None

      nextToken match {
        case Some(token) =>
          logger.info(s"next token=${token}")
          currToken = token
          self ! Status

        case None if updateFreq.nonEmpty =>
          logger.info("no more data is available, will check again in {}", updateFreq.get)
          context.system.scheduler.scheduleOnce(updateFreq.get, self, Status)

        case None =>
          logger.info("no more data is available, will not check again")
      }

    case Status if buf.size < threshold =>
      logger.debug(s"status message: buffer-size=${buf.size}, will request for more data")
      sendNextChunkRequest(currToken).map(FinishedToken.apply) pipeTo self

    case Status =>
      logger.debug(s"status message: buffer-size=${buf.size}")
      context.system.scheduler.scheduleOnce(1.seconds, self, Status)

    case NewData(data) =>
      buf += data
      tsvCounter += 1

    case GetData if buf.nonEmpty =>
      sender ! buf.dequeue()

    case GetData =>
      // do nothing since there are no elements in buffer

    case HttpResponseSuccess(t) =>
      // get point in time of token
      val decoded = Try(new org.joda.time.LocalDateTime(Tokens.decompress(t).takeWhile(_ != '|').toLong))
      logger.debug(s"successfully consumed token: $t point in time: ${decoded.getOrElse("")} buffer-size: ${buf.size}")
      currConsumeState = ConsumeStateHandler.nextSuccess(currConsumeState)

    case HttpResponseFailure(t, err) =>
      currConsumeState = ConsumeStateHandler.nextFailure(currConsumeState)
      logger.info(s"error: ${err.getMessage} consumer will perform retry in $retryTimeout, token=$t", err)
      after(retryTimeout, context.system.scheduler)(sendNextChunkRequest(t).map(FinishedToken.apply) pipeTo self)

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

      val to = lastBulkConsumeToHeader.map("&to-hint=" + _).getOrElse("")

      val uri = s"${formatHost(baseUrl)}/$consumeHandler?position=$token&format=tsv$paramsValue$slowBulk$to"
      logger.debug("send HTTP request: {}", uri)
      HttpRequest(uri = uri)
    }

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

          if (updateFreq.isEmpty) self ! NewData(None)

          None -> Source.empty
        case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.OK || s == StatusCodes.PartialContent =>
          val nextToken = getPosition(h) match {
            case Some(HttpHeader(_, pos)) => pos
            case None      => throw new RuntimeException("no position supplied")
          }

          lastBulkConsumeToHeader = getTo(h) match {
            case Some(HttpHeader(_, to)) => Some(to)
            case None                    => None
          }

          logger.info(s"received consume answer from host=${getHostnameValue(h)}")

          val dataSource: Source[(Token, Tsv), Any] = e.withoutSizeLimit().dataBytes
            .via(lineSeparatorFrame)
            .map(extractTsv)
            .map(token -> _)

          Some(nextToken) -> dataSource

        case (Success(HttpResponse(s, h, e, _)), _) =>
          e.toStrict(1.minute).onComplete {
            case Success(res:HttpEntity.Strict) =>
              logger.info(s"received consume answer from host=${getHostnameValue(h)} status=$s token=$token entity=${res.data.utf8String}")
            case Failure(err) =>
              logger.error(s"received consume answer from host=${getHostnameValue(h)} status=$s token=$token cannot extract entity", err)
          }

          Some(token) -> Source.failed(new Exception(s"Status is $s"))

        case x =>
          logger.error(s"unexpected message: $x")
          Some(token) -> Source.failed(new UnsupportedOperationException(x.toString))
      }
      .alsoToMat(Sink.last)((_,element) => element.map { case (nextToken, _) => nextToken})
      .map { case (_, dataSource) => dataSource}
      .flatMapConcat(identity)
      .collect { case (token, tsv: TsvData) =>
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

    result.onComplete {
      case Success(_)   => self ! HttpResponseSuccess(token)
      case Failure(err) => self ! HttpResponseFailure(token, err)
    }

    result
  }
}