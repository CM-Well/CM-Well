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
import akka.http.scaladsl.model.{ContentTypes, _}
import akka.stream.Supervision.Decider
import akka.stream.contrib.Retry
import akka.stream.{ActorAttributes, Materializer, Supervision}
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import akka.util.{ByteString, ByteStringBuilder}
import cmwell.dc.{LazyLogging, Settings}
import cmwell.dc.Settings._
import cmwell.dc.stream.MessagesTypesAndExceptions._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

/**
  * Created by eli on 23/06/16.
  */
object InfotonRetriever extends LazyLogging {

  type RetrieveInput = Seq[InfotonData]
  type RetrieveOutput = scala.collection.immutable.Seq[InfotonData]

  case class RetrieveStateStatus(retriesLeft: Int, lastException: Option[Throwable])

  val okStatus = RetrieveStateStatus(0, None)
  type RetrieveState = (RetrieveInput, RetrieveStateStatus)
  val initialRetrieveBulkStatus = RetrieveStateStatus(Settings.initialBulkRetrieveRetryCount, None)
  val initialRetrieveSingleStatus = RetrieveStateStatus(Settings.initialSingleRetrieveRetryCount, None)


  case class ParsedNquad(path: String, nquad: ByteString)

  case class RetrieveTotals(parsed: Map[String, ByteStringBuilder], unParsed: ByteStringBuilder)

  val breakOut = scala.collection.breakOut[RetrieveInput, InfotonData, RetrieveOutput]
  val breakOut2 = scala.collection.breakOut[RetrieveInput, (Future[RetrieveInput], RetrieveState), List[(Future[RetrieveInput], RetrieveState)]]

  //The path will be unique for each bulk infoton got into the flow
  def apply(dcInfo: DcInfo, decider: Decider)(implicit sys: ActorSystem, mat: Materializer): Flow[Seq[InfotonData], scala.collection.immutable.Seq[InfotonData], NotUsed] = {
    val remoteUri = "http://" + dcInfo.location
    val checkResponse = checkResponseCreator(dcInfo.id, dcInfo.location, decider) _
    val retrieveFlow: Flow[(Future[RetrieveInput], RetrieveState), (Try[RetrieveOutput], RetrieveState), NotUsed] =
      Flow[(Future[RetrieveInput], RetrieveState)]
        .mapAsync(1) { case (input, state) => input.map(_ -> state) }
        .map {
          case (infotonData, state) =>
            val payload = infotonData.foldLeft(ByteString(""))(_ ++ endln ++ ii ++ _.meta.uuid)
            val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, payload)
            entityToRequest(remoteUri, entity) -> state
        }
        .via(Http().superPool[RetrieveState]())
        .flatMapConcat {
          case (Success(response), state) if response.status.isSuccess() => {
            response.entity.dataBytes
              .via(Framing.delimiter(endln, maximumFrameLength = maxStatementLength))
              .fold(RetrieveTotals(state._1.map(im => (im.meta.path, new ByteStringBuilder)).toMap, new ByteStringBuilder)) { (totals, nquad) =>
                totals.unParsed ++= nquad
                val parsed: Option[ParsedNquad] = parseNquad(remoteUri, nquad)
                parsed.foreach {
                  p => totals.parsed.get(p.path) match {
                    case None => throw WrongPathGotException(s"Got path ${p.path} from _out that was not in the uuids request bulk: ${state._1.map(i => i.meta.uuid.utf8String + ":" + i.meta.path).mkString(",")}")
                    case Some(builder) => builder ++= (p.nquad ++ endln)
                  }
                }
                RetrieveTotals(totals.parsed, totals.unParsed)
              }
              .map { totals =>
                val parsedResult: Try[RetrieveOutput] = Success(state._1.map {
                  //todo: validity checks that the data arrived correctly. e.g. that the path could be retrieved from _out etc.
                  im => InfotonData(im.meta, totals.parsed(im.meta.path).result)
                }(breakOut))
                (parsedResult, state, Option(totals.unParsed.result))
              }
              .withAttributes(ActorAttributes.supervisionStrategy(decider))
              .recover {
                case ex: Throwable => (Failure[RetrieveOutput](ex), (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))), None)
              }
          }
          case (res@Success(HttpResponse(s, h, entity, _)), state) => {
            val errorID = res.##
            val e = new Exception(s"Error ![$errorID]. Cm-Well returned bad response: status: ${s.intValue} headers: ${Util.headersString(h)} reason: ${s.reason}.")
            val bodyFut = entity.dataBytes.runFold(empty)(_ ++ _).map(_.utf8String)
            val ex = RetrieveBadResponseException(s"Retrieve infotons failed. Data center ID ${dcInfo.id}, using remote location ${dcInfo.location} uuids: ${state._1.map(i => i.meta.uuid.utf8String).mkString(",")}.", bodyFut, e)
            //            logger.warn("Retrieve infotons failed.", ex)
            Source.single(Failure[RetrieveOutput](ex), (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))), None)
          }
          case (Failure(e), state) => {
            val ex = RetrieveException(s"Retrieve infotons failed. Data center ID ${dcInfo.id}, using remote location ${dcInfo.location} uuids: ${state._1.map(i => i.meta.uuid.utf8String).mkString(",")}", e)
            //              logger.warn("Retrieve infotons failed.", ex)
            Source.single(Failure[RetrieveOutput](ex), (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))), None)
          }
        }
        .map(checkResponse)

    Flow[RetrieveInput]
      .map(input => Future.successful(input) -> (input -> initialRetrieveBulkStatus))
      .via(Retry.concat(Settings.retrieveRetryQueueSize, retrieveFlow)(retryDecider(dcInfo.id, dcInfo.location)))
      .map {
        case (Success(data), _) => data
        case (Failure(e), _) => {
          logger.error(s"Data Center ID ${dcInfo.id} from location ${dcInfo.location}. Retrieve should never fail after retry.", e)
          throw e
        }
      }
  }

  def parseNquad(remoteUri: String, nquadLine: ByteString): Option[ParsedNquad] = {
    val line = nquadLine.utf8String.replace(remoteUri, cmwellPrefix)
    val wrappedSubject = line.takeWhile(_ != space)
    if (wrappedSubject.length > 1 &&
      !wrappedSubject.startsWith("_:") &&
      !line.contains("/meta/sys#uuid") &&
      !line.contains("/meta/sys#parent") &&
      !line.contains("/meta/sys#path")) {
      val uri = wrappedSubject.tail.init
      val path = cmwell.domain.FReference(uri).getCmwellPath
      val nquad = ByteString(line)
      Some(ParsedNquad(path, nquad))
    }
    else None
  }

  def entityToRequest(dst: String, entity: RequestEntity) =
    HttpRequest(method = HttpMethods.POST, uri = s"$dst/_out?format=nquads", entity = entity)

  def checkResponseCreator(dataCenterId: String, location: String, decider: Decider)(response: (Try[RetrieveOutput], RetrieveState, Option[ByteString])): (Try[RetrieveOutput], RetrieveState) =
    response match {
      case (response@Success(res), state@(input, status), body) => {
        val missingUuids = res.filter(_.data == empty)
        if (missingUuids.isEmpty) {
          status.lastException.foreach { e =>
            val bulkCount = if (input.size > 1) Settings.initialBulkRetrieveRetryCount - status.retriesLeft + 1 else Settings.initialBulkRetrieveRetryCount + 1
            val singleCount = if (input.size > 1) 0 else Settings.initialSingleRetrieveRetryCount - status.retriesLeft + 1
            yellowlog.info(s"Retrieve succeeded only after $bulkCount bulk retrieves and $singleCount single infoton retrieves. uuids: ${input.map(i => i.meta.uuid.utf8String).mkString(",")}.")
          }
          (response, state)
        }
        else {
          val errorID = response.##
          val ex = RetrieveException(s"Error ![$errorID]. Retrieve infotons failed. Some infotons don't have data. Data center ID $dataCenterId, using remote location $location missing uuids: ${missingUuids.map(i => i.meta.uuid.utf8String).mkString(",")}.")
          logger.debug(s"Error ![$errorID]. Full response body was: $body")
          (Failure(ex), (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))))
        }
      }
      case (fail@Failure(e), state, _) => (fail, state)
    }

  def retryDecider(dataCenterId: String, location: String)(implicit sys: ActorSystem, mat: Materializer) = (state: RetrieveState) => state match {
    case (ingestSeq, RetrieveStateStatus(retriesLeft, ex)) =>
      if (ingestSeq.size == 1 && retriesLeft == 0) {
        ex.get match {
          case e: FuturedBodyException =>
            logger.error(s"${e.getMessage} ${e.getCause.getMessage} No more retries will be done. Please use the red log to see the list of all the failed retrievals.")
            Util.errorPrintFuturedBodyException(e)
          case e => logger.error(s"Data Center ID $dataCenterId: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} from $location failed. No more reties will be done. Please use the red log to see the list of all the failed retrievals. The exception is: ", e)
        }
        redlog.info(s"Data Center ID $dataCenterId: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} from $location failed.")
        Some(List.empty[(Future[RetrieveInput], RetrieveState)])
      }
      else if (ingestSeq.size == 1) {
        logger.trace(s"Data Center ID $dataCenterId: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} from $location failed. Retries left $retriesLeft. Will try again. The exception is: ", ex.get)
        Util.tracePrintFuturedBodyException(ex.get)
        val ingestState = (ingestSeq, RetrieveStateStatus(retriesLeft - 1, ex))
        val delay = Settings.maxSingleRetrieveRetryDelay.div(retriesLeft)
        Some(List(akka.pattern.after(delay, sys.scheduler)(Future.successful(ingestSeq)) -> ingestState))
      }
      else if (retriesLeft == 0) {
        logger.trace(s"Data Center ID $dataCenterId: Retrieve of bulk uuids from $location failed. No more bulk retries left. Will split to request for each uuid and try again. The exception is: ", ex.get)
        Util.tracePrintFuturedBodyException(ex.get)
        Some(ingestSeq.map { infotonMetaAndData =>
          val ingestData = Seq(infotonMetaAndData)
          val ingestState = ingestData -> RetrieveStateStatus(Settings.initialSingleRetrieveRetryCount, ex)
          val delay = Settings.maxSingleRetrieveRetryDelay.div(Settings.initialSingleRetrieveRetryCount)
          akka.pattern.after(delay, sys.scheduler)(Future.successful(ingestData)) -> ingestState
        }(breakOut2))
      }
      else {
        logger.trace(s"Data Center ID $dataCenterId: Retrieve of bulk uuids from $location failed. Retries left $retriesLeft. Will try again. The exception is: ", ex.get)
        Util.tracePrintFuturedBodyException(ex.get)
        val ingestState = (ingestSeq, RetrieveStateStatus(retriesLeft - 1, ex))
        val delay = Settings.maxBulkRetrieveRetryDelay.div(retriesLeft)
        Some(List(akka.pattern.after(delay, sys.scheduler)(Future.successful(ingestSeq)) -> ingestState))
      }
  }
}
