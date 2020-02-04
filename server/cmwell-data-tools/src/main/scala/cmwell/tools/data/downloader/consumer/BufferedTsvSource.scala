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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.stage._
import cmwell.tools.data.downloader.consumer.Downloader._
import cmwell.tools.data.downloader.consumer.BufferedTsvSource.{ConsumeResponse, SensorOutput}
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.{HttpAddress, formatHost}
import cmwell.tools.data.utils.akka.HeaderOps.{getHostnameValue, getNLeft, getPosition}
import cmwell.tools.data.utils.akka.{AkkaUtils, DataToolsConfig, HttpConnections, lineSeparatorFrame}
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Tokens

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object BufferedTsvSource {

  type SensorOutput = ((Token,TsvData), Boolean, Option[Long])
  case class AgentSensorOutput (agent: String, sensorOutput: SensorOutput)

  case class ConsumeResponse(token: Option[String], consumeComplete: Boolean, infotonSource: Source[(Token, Tsv), Any])

  def apply(initialToken: Future[String],
            threshold: Long ,
            params: String = "",
            baseUrl: String,
            consumeLengthHint: Option[Int],
            retryTimeout: FiniteDuration,
            horizonRetryTimeout: FiniteDuration ,
            isBulk: Boolean = false,
            label: Option[String] = None)(implicit system: ActorSystem, ec: ExecutionContext, mat: Materializer)
  = new BufferedTsvSource(initialToken, threshold, params, baseUrl, consumeLengthHint, retryTimeout, horizonRetryTimeout, isBulk, label)
}

class BufferedTsvSource(initialToken: Future[String],
                        threshold : Long,
                        params: String = "",
                        baseUrl: String,
                        consumeLengthHint: Option[Int],
                        retryTimeout: FiniteDuration,
                        horizonRetryTimeout: FiniteDuration,
                        isBulk: Boolean,
                        label: Option[String] = None)(implicit system: ActorSystem, ec: ExecutionContext, mat: Materializer)
  extends GraphStage[SourceShape[SensorOutput]] with DataToolsLogging with DataToolsConfig {

  val out = Outlet[SensorOutput]("sensor.out")
  override val shape = SourceShape(out)

  def isHorizon(consumeComplete: Boolean, buf: mutable.Queue[Option[(Token, TsvData)]]) = consumeComplete && buf.isEmpty

  def decodeToken(token: Token) = Try(
    new org.joda.time.LocalDateTime(
      Tokens.decompress(token).takeWhile(_ != '|').toLong
    )
  )

  private val HttpAddress(protocol, host, port, _) = ArgsManipulations.extractBaseUrl(baseUrl)
  private val conn = HttpConnections.newHostConnectionPool[Option[_]](host, port, protocol)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var callback : AsyncCallback[(Seq[(Token,Tsv)], Option[Token])] = _
    var asyncCallInProgress = false

    private var currentConsumeToken: Token = _
    private var consumeComplete = false
    private var remainingInfotons : Option[Long] = None
    private var buf: mutable.Queue[Option[(Token, TsvData)]] = mutable.Queue()
    private var currConsumeState: ConsumeState = SuccessState(0)

    private val changeInProgressState = getAsyncCallback[Boolean](asyncCallInProgress = _)
    private val changeCurrentConsumeTokenState = getAsyncCallback[Token](currentConsumeToken = _)
    private val changeConsumeCompleteState = getAsyncCallback[Boolean](consumeComplete = _)
    private val changeRemainingInfotonsState = getAsyncCallback[Option[Long]](remainingInfotons = _)
    private val addToBuffer = getAsyncCallback[Option[(Token, TsvData)]](buf += _)
    private val changeCurrConsumeState = getAsyncCallback[ConsumeState](currConsumeState = _)
    private var stopStream: AsyncCallback[Unit] = _

    val keepAlive = config.getBoolean("downloader.keepAlive")

    override def preStart(): Unit = {

      def bufferFillerCallback(infotonsAndNextToken :(Seq[(Token,Tsv)], Option[Token])) = infotonsAndNextToken match {
        case (infotons, Some(nextToken)) =>
          infotons.foreach {
            case (token, tsvData: TsvData) => addToBuffer.invoke((Some(token, tsvData)))
            case (token, TsvEmpty) => logger.error(s"Got TsvEmpty for: $token"); ???
          }

          changeCurrentConsumeTokenState.invokeWithFeedback(nextToken).flatMap({ _ =>
            changeInProgressState.invokeWithFeedback(false).map({ _ =>
              logger.debug(s"successfully consumed token: $currentConsumeToken point in time: ${
                decodeToken(currentConsumeToken).getOrElse("")
              }  buffer-size: ${buf.size}")
              //logger.warn("OnPull Triggered")
              getHandler(out).onPull()
            })
          })

        case (infotons, None) => logger.error(s"Token is None for: $infotons"); ???
      }

      stopStream = getAsyncCallback[Unit] ( _ => {
        logger.info("Going to close source")
        val iterable = buf.map(elem => (elem.get, true, None)).iterator
        emitMultiple(out, iterable)
        complete(out)
      }
      )

      currentConsumeToken = Await.result(initialToken, 5.seconds)

      callback = getAsyncCallback[(Seq[(Token,Tsv)], Option[Token])](bufferFillerCallback)
    }

    override def postStop(): Unit = {
      logger.info(s"Buffered TSV Source stopped: buffer-size: ${buf.size}, label: $label")
    }



    /**
      * Sends request of next data chunk and fills the given buffer
      * @param token cm-well position token to consume its data
      * @return optional next token value, otherwise None when there is no data left to be consumed
      */
    def sendNextChunkRequest(token: String): Future[ConsumeResponse] = {

      /**
        * Creates http request for consuming data
        * @param token position token to be consumed
        * @param toHint to-hint field of cm-well consumer API
        * @return HTTP request for consuming data
        */
      def createRequestFromToken(token: String, toHint: Option[String] = None) = {
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

        val lengthHintStr = consumeLengthHint.fold("") { chunkSize => "&length-hint=" + chunkSize }

        val uri =
          s"${formatHost(baseUrl)}/$consumeHandler?position=$token&format=tsv$paramsValue$slowBulk$lengthHintStr"

        logger.debug("send HTTP request: {}", uri)
        HttpRequest(uri = uri).addHeader(RawHeader("Accept-Encoding", "gzip"))
      }

      val src: Source[ConsumeResponse,NotUsed] =
        Source
          .single(createRequestFromToken(token, None))
          .map(_ -> None)
          .via(conn)
          .map {
            case (tryResponse, state) =>
              tryResponse.map(AkkaUtils.decodeResponse) -> state
          }
          .map {
            case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.TooManyRequests =>
              e.discardBytes()

              logger.error(s"HTTP 429: too many requests token=$token")

              ConsumeResponse(token = None, consumeComplete = false, infotonSource =
                Source.failed(new Exception("HTTP 429: too many requests")))

            case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.NoContent =>
              e.discardBytes()

              logger.info(s"No more data available, setting consume complete to true")

              ConsumeResponse(token=None, consumeComplete = true, infotonSource = Source.empty)

            case (Success(HttpResponse(s, h, e, _)), _) if s == StatusCodes.OK || s == StatusCodes.PartialContent =>
              getPosition(h) match {
                case Some(HttpHeader(_, pos)) =>

                  changeRemainingInfotonsState.invoke(
                    getNLeft(h) match {
                      case Some(HttpHeader(_, nLeft)) => Some(nLeft.toInt)
                      case _ => None
                    }
                  )

                  logger.debug(s"received consume answer from host=${getHostnameValue(h)}")

                  val infotonSource: Source[(Token, Tsv), Any] = e
                    .withoutSizeLimit()
                    .dataBytes
                    .via(lineSeparatorFrame)
                    .map(extractTsv)
                    .map(token -> _)

                  ConsumeResponse(token=Some(pos), consumeComplete = false, infotonSource = infotonSource)

                case None =>
                  e.discardBytes()
                  ConsumeResponse(token=Some(token), consumeComplete = false, infotonSource =
                    Source.failed(new Exception(s"No position supplied")))
              }

            case (Success(HttpResponse(s, h, e, _)), _) =>
              e.toStrict(1.minute).onComplete {
                case Success(res: HttpEntity.Strict) =>

                  changeCurrConsumeState.invoke(ConsumeStateHandler.nextSuccess(currConsumeState))

                  logger
                    .info(
                      s"received consume answer from host=${getHostnameValue(
                        h
                      )} status=$s token=$token entity=${res.data.utf8String}"
                    )
                case Failure(err) =>
                  changeCurrConsumeState.invoke( ConsumeStateHandler.nextFailure(currConsumeState))

                  logger.error(
                    s"received consume answer from host=${getHostnameValue(h)} status=$s token=$token cannot extract entity",
                    err
                  )
              }

              ConsumeResponse(token=Some(token), consumeComplete = false, infotonSource =
                Source.failed(new Exception(s"Status is $s")))

            case x =>
              logger.error(s"unexpected response: $x")

              ConsumeResponse(token=Some(token), consumeComplete = false, infotonSource =
                Source.failed(
                  new UnsupportedOperationException(x.toString)
                ))
          }

      src.toMat(Sink.head)(Keep.right).run()

    }

    setHandler(out, new OutHandler {


      override def onDownstreamFinish() : Unit = {
        logger.info(s"Downstream finished: buffer-size: ${buf.size}, label: $label")
      }


      override def onPull(): Unit = {
        if (buf.nonEmpty){
          buf.dequeue().foreach(tokenAndData=>{
            val sensorOutput = ((tokenAndData._1, tokenAndData._2), isHorizon(consumeComplete,buf), remainingInfotons)
            logger.debug(s"successfully de-queued tsv: $currentConsumeToken remaining buffer-size: ${buf.size}")
            emit(out,sensorOutput)
          })
        }

        if(buf.size < threshold && !asyncCallInProgress && isHorizon(consumeComplete,buf)==false){
          asyncCallInProgress = true
          logger.debug(s"buffer size: ${buf.size} is less than threshold of $threshold. Requesting more tsvs")
          invokeBufferFillerCallback(sendNextChunkRequest(currentConsumeToken), retryLimit.get)
        }
      }
    })

    private def invokeBufferFillerCallback(future: Future[ConsumeResponse], reqRetryLimit: Int): Unit = {

      future.onComplete {
        case Success(consumeResponse) =>

          changeConsumeCompleteState.invoke(consumeResponse.consumeComplete)

          consumeResponse match {
            case ConsumeResponse(_, true, _) =>

              if (keepAlive)
              {
                logger.info(s"$label is at horizon. Will retry at consume position $currentConsumeToken " +
                s"in $horizonRetryTimeout")

              materializer.scheduleOnce(horizonRetryTimeout, () =>
                invokeBufferFillerCallback(sendNextChunkRequest(currentConsumeToken), retryLimit.get))
              }
              else
                stopStream.invoke()

            case ConsumeResponse(nextToken ,false, infotonSource) =>
              infotonSource.toMat(Sink.seq)(Keep.right).run
                .onComplete {
                  case Success(consumedInfotons) =>
                    callback.invoke((consumedInfotons, nextToken))
                  case Failure(ex) =>
                    if (reqRetryLimit > 0) {
                      logger.error(s"Received error. scheduling a retry in $retryTimeout", ex)
                      materializer.scheduleOnce(retryTimeout, () =>
                        invokeBufferFillerCallback(sendNextChunkRequest(currentConsumeToken), reqRetryLimit-1)
                      )
                    }
                    else
                    {
                      logger.error("Retry limit has exceeded. Going to close stream without completing all TSVs.", ex)
                      stopStream.invoke()
                    }
                }
          }
        case Failure(e) =>
          logger.error(s"TSV source future failed", e)
          throw e
      }

    }
  }

}
