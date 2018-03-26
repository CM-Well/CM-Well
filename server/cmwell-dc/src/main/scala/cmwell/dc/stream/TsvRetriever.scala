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
package cmwell.dc.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{`Accept-Encoding`, HttpEncodings}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.{ActorAttributes, KillSwitch, KillSwitches, Materializer}
import akka.stream.Supervision.Decider
import akka.stream.contrib.{Retry, SourceGen}
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import akka.util.ByteString
import cmwell.dc.{LazyLogging, Settings}
import cmwell.dc.Settings._
import cmwell.dc.stream.MessagesTypesAndExceptions._
import cmwell.dc.stream.akkautils.DebugStage
import com.typesafe.config.ConfigFactory

import scala.collection.parallel.immutable
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by eli on 19/07/16.
  */
object TsvRetriever extends LazyLogging {
  val gzipAcceptEncoding = `Accept-Encoding`(HttpEncodings.gzip)
  val maxTsvLineLength = {
    val slashHttpsDotPossiblePrefix = "/https.".length
    val maxUrlLength = 2083
    val dateLength = 25
    val uuidLength = 32
    val indexTimeLength = 13
    val tabs = 3
    slashHttpsDotPossiblePrefix + maxUrlLength + dateLength + uuidLength + indexTimeLength + indexTimeLength + 1
  }

  def parseTSVAndCreateInfotonDataFromIt(tsv: ByteString) = {
    val tabAfterPath = tsv.indexOfSlice(tab)
    val tabAfterLast = tsv.indexOfSlice(tab, tabAfterPath + 1)
    val tabAfterUuid = tsv.indexOfSlice(tab, tabAfterLast + 1)

    val path = tsv.take(tabAfterPath).utf8String
    val uuid = tsv.slice(tabAfterLast + 1, tabAfterUuid)
    val idxt = tsv.drop(tabAfterUuid).utf8String.trim.toLong

    logger.trace(
      s"parseTSVAndCreateInfotonDataFromIt: [path='$path',uuid='${uuid.utf8String}',idxt='$idxt']"
    )
    InfotonData(InfotonMeta(path, uuid, idxt), empty)
  }

  sealed trait ConsumeType

  object BulkConsume extends ConsumeType {
    override def toString = "BulkConsume"
  }

  object Consume extends ConsumeType {
    override def toString = "Consume"
  }

  case class TsvFlowOutput(tsvs: List[InfotonData],
                           nextPositionKey: String,
                           isNoContent: Boolean = false)

  case class ConsumeState(op: ConsumeType, startTime: Long)

  case class TsvFlowState(tsvRetrieveInput: TsvRetrieveInput,
                          retriesLeft: Int,
                          lastException: Option[Throwable],
                          consumeState: ConsumeState)

  type TsvRetrieveInput = String
  type TsvRetrieveOutput = TsvFlowOutput
  type TsvRetrieveState = TsvFlowState

  def apply(dcInfo: DcInfo, decider: Decider)(
    implicit mat: Materializer,
    system: ActorSystem
  ): Source[List[InfotonData], (KillSwitch, Future[Seq[Option[String]]])] = {
    SourceGen.unfoldFlowWith(
      Future.successful(dcInfo.positionKey.get),
      retrieveTsvsWithRetryAndLastPositionKey(dcInfo, decider)
    ) {
      case Success(TsvFlowOutput(tsvs, nextPositionKey, isNoContent))
          if !isNoContent => {
        Some(Future.successful(nextPositionKey), tsvs)
      }
      case Success(TsvFlowOutput(tsvs, nextPositionKey, isNoContent)) => {
        logger.info(
          s"Data Center ID ${dcInfo.id} from ${dcInfo.location}. Got 204 no content. Will close the stream. It will be opened on the next scheduled check."
        )
        /* - cancelled due to errors in akka stream. the stream will be closed and opened again for each 204
                val f = akka.pattern.after(Settings.delayInSecondsBetweenNoContentRetries.seconds, system.scheduler)(Future.successful(nextPositionKey))
                logger.info(s"The type of 204 Future is $f")
                f.onComplete(pos => logger.info(s"204 next position key future finished with $f"))
                //val f = cmwell.util.concurrent.delayedTask(Settings.delayInSecondsBetweenNoContentRetries.seconds)(nextPositionKey)(system.dispatcher)
                Some(f, List())
         */
        None
      }
      case Failure(ex) => {
        logger.error(s"Data Center ID ${dcInfo.id}: Retrieve of TSVs from ${dcInfo.location} failed. " +
                     s"Completing the stream (current got TSVs should be ok unless another exception is caught later). The exception is:",ex)
        None
      }
    }
  }

  def retrieveTsvsWithRetryAndLastPositionKey(
    dcInfo: DcInfo,
    decider: Decider
  )(implicit mat: Materializer, system: ActorSystem): Flow[Future[String], Try[
    TsvRetrieveOutput
  ], (KillSwitch, Future[Seq[Option[String]]])] = {
    //another sink to keep the last position got
    //the reason for sliding(2) is that the last element can be None (a stream can finish with an error) and then the element before should be taken
    val positionKeySink = Flow
      .fromFunction[Try[TsvRetrieveOutput], Option[String]] {
        case Success(TsvFlowOutput(tsvs, nextPositionKey, isNoContent)) =>
          Some(nextPositionKey)
        case _ => None
      }
      .sliding(2)
      .toMat(Sink.last)(Keep.right)
    retrieveTsvsWithRetry(dcInfo, decider).alsoToMat(positionKeySink)(Keep.both)
  }

  def retrieveTsvsWithRetry(dcInfo: DcInfo, decider: Decider)(
    implicit mat: Materializer,
    system: ActorSystem
  ): Flow[Future[String], Try[TsvRetrieveOutput], KillSwitch] =
    Flow[Future[String]]
      .mapAsync(1)(identity)
      .viaMat(KillSwitches.single)(Keep.right)
      .map(
        positionKey =>
          Future.successful(positionKey) -> TsvFlowState(
            positionKey,
            Settings.initialTsvRetryCount,
            None,
            ConsumeState(BulkConsume, System.currentTimeMillis)
        )
      )
      .via(
        Retry(retrieveTsvFlow(dcInfo, decider))(
          retryDecider(dcInfo.id, dcInfo.location)
        )
      )
      .map(_._1)

  private def stayInThisState(stateStartTime: Long): Boolean =
    System.currentTimeMillis - stateStartTime < Settings.consumeFallbackDuration

  private def extractPrefixes(state: ConsumeState) = state match {
    case ConsumeState(BulkConsume, _) => "bulk-"
    case ConsumeState(Consume, _)     => ""
  }

  private def getNewState(elementState: TsvRetrieveState,
                          lastUsedState: ConsumeState) =
    // If there were an error before - take the state as it came from the retry decider.
    // else change from lower consume type to a better one only after the time interval.
    if (elementState.lastException.isDefined) elementState.consumeState
    else
      lastUsedState match {
        case state @ ConsumeState(BulkConsume, _) => state
        case state @ ConsumeState(Consume, start) =>
          if (stayInThisState(start)) state
          else ConsumeState(BulkConsume, System.currentTimeMillis)
      }

  def retrieveTsvFlow(dcInfo: DcInfo, decider: Decider)(
    implicit mat: Materializer,
    system: ActorSystem
  ): Flow[(Future[TsvRetrieveInput], TsvRetrieveState),
          (Try[TsvRetrieveOutput], TsvRetrieveState),
          NotUsed] = {
    val startTime = System.currentTimeMillis
    val hostPort = dcInfo.location.split(":")
    val (host, port) = hostPort.head -> hostPort.tail.headOption
      .getOrElse("80")
      .toInt
    val tsvPoolConfig = ConfigFactory
      .parseString("akka.http.host-connection-pool.max-connections=1")
      .withFallback(config)
    val tsvConnPool = Http()
      .newHostConnectionPool[TsvRetrieveState](
        host,
        port,
        ConnectionPoolSettings(tsvPoolConfig)
      )
    Flow[(Future[TsvRetrieveInput], TsvRetrieveState)]
      .mapAsync(1) { case (input, state) => input.map(_ -> state) }
      .statefulMapConcat { () =>
        var currentState = ConsumeState(BulkConsume, System.currentTimeMillis);
        {
          case (positionKey, state) =>
            currentState = getNewState(state, currentState)
            val bulkPrefix = extractPrefixes(currentState)
            val request = HttpRequest(
              uri =
                s"http://${dcInfo.location}/?op=${bulkPrefix}consume&format=tsv&position=$positionKey",
              headers = scala.collection.immutable.Seq(gzipAcceptEncoding)
            )
            logger.info(
              s"Data Center ID ${dcInfo.id}: Sending ${currentState.op} request to ${dcInfo.location} using position key $positionKey."
            )
            scala.collection.immutable.Seq(
              request -> state.copy(consumeState = currentState)
            )
        }
      }
      .via(tsvConnPool)
      .map {
        case (tryResponse, state) =>
          tryResponse.map(Util.decodeResponse) -> state
      }
      .flatMapConcat {
        case (Success(res @ HttpResponse(s, h, entity, _)), state)
            if s.isSuccess() && h.exists(_.name == "X-CM-WELL-POSITION") => {
          val nextPositionKey = res.getHeader("X-CM-WELL-POSITION").get.value()
          entity.dataBytes
            .via(
              Framing
                .delimiter(endln, maximumFrameLength = maxTsvLineLength * 2)
            )
            .fold(List[InfotonData]())(
              (total, bs) => parseTSVAndCreateInfotonDataFromIt(bs) :: total
            )
            .map { data =>
              val sortedData = data.sortBy(_.meta.indexTime)
              if (state.retriesLeft < Settings.initialTsvRetryCount) {
                val consumeCount = Settings.initialTsvRetryCount - state.retriesLeft + 1
                yellowlog.info(
                  s"TSV (bulk)consume succeeded only after $consumeCount (bulk)consumes. token: ${state.tsvRetrieveInput}."
                )
              }
              Success(
                TsvFlowOutput(sortedData, nextPositionKey, s.intValue == 204)
              ) -> state.copy(lastException = None)
            }
            .withAttributes(ActorAttributes.supervisionStrategy(decider))
            .recover {
              case e =>
                val ex = RetrieveTsvException(
                  s"Retrieve TSVs using ${state.consumeState.op} failed. Data center ID ${dcInfo.id}, using remote location ${dcInfo.location}",
                  e
                )
                logger.warn("Retrieve TSVs failed.", ex)
                Failure[TsvRetrieveOutput](ex) -> state.copy(
                  lastException = Some(ex)
                )
            }
        }
        case (res @ Success(HttpResponse(s, h, entity, _)), state) => {
          val errorID = res.##
          val e = new Exception(
            s"Error ![$errorID]. Cm-Well returned bad response: status: ${s.intValue} headers: ${Util
              .headersString(h)} reason: ${s.reason}"
          )
          val bodyFut =
            entity.dataBytes.runFold(empty)(_ ++ _).map(_.utf8String)
          val ex = RetrieveTsvBadResponseException(
            s"Retrieve TSVs using ${state.consumeState.op} failed. Data center ID ${dcInfo.id}, using remote location ${dcInfo.location}.",
            bodyFut,
            e
          )
          logger.warn(s"${ex.getMessage} ${ex.getCause.getMessage}")
          Util.warnPrintFuturedBodyException(ex)
          Source.single(
            Failure[TsvRetrieveOutput](ex) -> state
              .copy(lastException = Some(ex))
          )
        }
        case (Failure(e), state) => {
          val ex = RetrieveTsvException(
            s"Retrieve TSVs using ${state.consumeState.op} failed. Data center ID ${dcInfo.id}, using remote location ${dcInfo.location}",
            e
          )
          logger.warn("Retrieve TSVs failed.", ex)
          Source.single(
            Failure[TsvRetrieveOutput](ex) -> state
              .copy(lastException = Some(ex))
          )
        }
      }
      .statefulMapConcat { () =>
        var infotonsGot: Long = 0;
        {
          case output @ (
                Success(TsvFlowOutput(tsvs, nextPositionKey, _)),
                state
              ) =>
            infotonsGot += tsvs.size
            val rate = infotonsGot / ((System.currentTimeMillis - startTime) / 1000D)
            val d = dcInfo.id
            val s = tsvs.size
            val o = state.consumeState.op
            val r = rate.formatted("%.2f")
            logger.info(s"Data Center ID $d: Got TSVs stream source. The next position key to consume is $nextPositionKey. " +
                        s"Got $s TSVs using $o. Total TSVs got $infotonsGot. Read rate: $r TSVs/second")
            scala.collection.immutable.Seq(output)
          case output => scala.collection.immutable.Seq(output)
        }
      }
  }

  private def retryDecider(
    dataCenterId: String,
    location: String
  )(implicit mat: Materializer, system: ActorSystem) =
    (state: TsvRetrieveState) =>
      state match {
        case TsvFlowState(_, 0, _, _) =>
          // scalastyle:off
          logger.error(s"Data Center ID $dataCenterId: Retrieve of TSVs from $location failed. No more reties will be done. The sync will be closed now (no more new TSV will be got) and restarted again automatically.")
          // scalastyle:on
          None
        case TsvFlowState(positionKey, retriesLeft, ex, consumeState) =>
          val waitSeconds = ex match {
            case Some(_: RetrieveTsvBadResponseException) => 1
            // due to what seems to be a bug in akka http if there were an error during the retrieve of the body
            // (e.g. connection reset by peer) and another request is sent
            // the akka-http is stuck. To overcome this issue a wait of 40 seconds is added to allow the connection pool
            // to be properly closed before sending another request
            case Some(_) => 40
            case None =>
              ??? // Shouldn't get here. The retry decider is called only when there is an exception and the ex should be in the state
          }
          val newConsumeOp = consumeState.op match {
            case BulkConsume
                if Settings.initialTsvRetryCount - retriesLeft < Settings.bulkTsvRetryCount =>
              BulkConsume
            case _ => Consume
          }
          logger.warn(
            s"Data Center ID $dataCenterId: Retrieve of TSVs from $location failed. Retries left $retriesLeft. Will try again in $waitSeconds seconds."
          )
          Some(
            akka.pattern.after(waitSeconds.seconds, system.scheduler)(
              Future.successful(positionKey)
            ) -> TsvFlowState(
              positionKey,
              retriesLeft - 1,
              ex,
              ConsumeState(newConsumeOp, System.currentTimeMillis)
            )
          )
    }
}
