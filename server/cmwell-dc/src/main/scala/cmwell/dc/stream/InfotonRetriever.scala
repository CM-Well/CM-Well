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
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.headers.{`Accept-Encoding`, `Content-Encoding`, HttpEncodings}
import akka.http.scaladsl.model.{ContentTypes, _}
import akka.stream.Supervision.Decider
import akka.stream.contrib.Retry
import akka.stream.{ActorAttributes, Materializer, Supervision}
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import akka.util.{ByteString, ByteStringBuilder}
import cmwell.dc.{LazyLogging, Settings}
import cmwell.dc.Settings._
import cmwell.dc.stream.MessagesTypesAndExceptions._
import cmwell.util.akka.http.HttpZipDecoder
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import scala.collection.mutable.HashMap

/**
  * Created by eli on 23/06/16.
  */
object InfotonRetriever extends LazyLogging {

  type RetrieveInput = Seq[InfotonData]
  //Sequence of (InfotonData, Option[Read indexTime from cassandra(_out) )
  type RetrieveOutput =
    scala.collection.immutable.Seq[(InfotonData, ExtraData)]

  case class RetrieveStateStatus(retriesLeft: Int, lastException: Option[Throwable])

  val okStatus = RetrieveStateStatus(0, None)
  type RetrieveState = (RetrieveInput, RetrieveStateStatus)
  val initialRetrieveBulkStatus =
    RetrieveStateStatus(Settings.initialBulkRetrieveRetryCount, None)
  val initialRetrieveSingleStatus =
    RetrieveStateStatus(Settings.initialSingleRetrieveRetryCount, None)

  case class ExtraData(subject: String, indexTime: Option[Long], lastModifiedBy: Option[String]){
    def shouldMerge (other: ExtraData): Option[ExtraData] = {
      require(other.subject != "", "Subject in ExtraData cannot be empty!")
      require(!(subject!="" && other.subject != subject), "Same path cannot have different subjects!")

      other match {
        case ExtraData(newSubject, None, None) if this.subject == "" => Some(this.copy(subject = newSubject))
        case ExtraData(_, None, None) => None
        case ExtraData(newSubject, indTime @ Some(_), None) => Some(this.copy(subject = newSubject, indexTime = indTime))
        case ExtraData(newSubject, None, lmb @ Some(_)) => Some(this.copy(subject = newSubject, lastModifiedBy = lmb))
      }
    }

    def getQuads(indexTimeN: Long, modifierN: String) = {
      indexTime.fold(s"""<$subject> <cmwell://meta/sys#indexTime> "$indexTimeN"^^<http://www.w3.org/2001/XMLSchema#long> .""")(_ => "") ++
        lastModifiedBy.fold(s"""<$subject> <cmwell://meta/sys#lastModifiedBy> "$modifierN"^^<http://www.w3.org/2001/XMLSchema#string> .""")(_ => "")
    }
  }

  case class ParsedNquad(path: String, nquad: ByteString, extraData: ExtraData)

  case class RetrieveTotals(
    parsed: HashMap[String, (ByteStringBuilder, ExtraData)],
    unParsed: ByteStringBuilder
  )

  val breakOut = scala.collection
    .breakOut[RetrieveInput, (InfotonData, ExtraData), RetrieveOutput]
  val breakOut2 = scala.collection
    .breakOut[RetrieveInput, (Future[RetrieveInput], RetrieveState), List[
      (Future[RetrieveInput], RetrieveState)
    ]]
  val hashMapBreakout = scala.collection
    .breakOut[RetrieveInput, (String, (ByteStringBuilder, ExtraData)), HashMap[String, (ByteStringBuilder, ExtraData)]]

  //The path will be unique for each bulk infoton got into the flow
  def apply(dcKey: DcInfoKey, decider: Decider)(implicit sys: ActorSystem, mat: Materializer): Flow[Seq[
    InfotonData
  ], scala.collection.immutable.Seq[InfotonData], NotUsed] = {
    val remoteUri = "http://" + dcKey.location
    val checkResponse = checkResponseCreator(dcKey, decider) _
    val retrieveFlow: Flow[(Future[RetrieveInput], RetrieveState), (Try[RetrieveOutput], RetrieveState), NotUsed] =
      Flow[(Future[RetrieveInput], RetrieveState)]
        .mapAsync(1) { case (input, state) => input.map(_ -> state) }
        .map {
          case (infotonData, state) =>
            val payload = infotonData.foldLeft(ByteString(""))(
              _ ++ endln ++ ii ++ _.meta.uuid
            )
            createRequest(remoteUri, payload) -> state
        }
        .via(Http().superPool[RetrieveState]())
        .map {
          case (tryResponse, state) =>
            tryResponse.map(HttpZipDecoder.decodeResponse) -> state
        }
        .flatMapConcat {
          case (Success(response), state) if response.status.isSuccess() => {
            response.entity.dataBytes
              .via(
                Framing.delimiter(endln, maximumFrameLength = maxStatementLength)
              )
              .fold(RetrieveTotals(state._1.map{im => im.meta.path -> (new ByteStringBuilder, ExtraData("", None, None))}(hashMapBreakout),
                new ByteStringBuilder))
              { (totals, nquad) =>
                totals.unParsed ++= nquad
                val parsed: Option[ParsedNquad] = parseNquad(remoteUri, nquad)
                parsed.fold(RetrieveTotals(totals.parsed, totals.unParsed)){case ParsedNquad(path, nquad, extraData) =>
                  totals.parsed.get(path) match {
                    case None =>
                      throw WrongPathGotException(s"Got path ${path} from _out that was not in the uuids request bulk: ${
                        state._1.map(i => i.meta.uuid.utf8String + ":" + i.meta.path).mkString(",")}")
                    case Some((builder, curExtraData)) => {
                      builder ++= (nquad ++ endln)
                      curExtraData.shouldMerge(extraData).foreach(ex => totals.parsed.put(path, (builder, ex)))
                      RetrieveTotals(totals.parsed, totals.unParsed)
                    }
                  }
                }
              }
              .map { totals =>
                val parsedResult: Try[RetrieveOutput] = Success(state._1.map {
                  //todo: validity checks that the data arrived correctly. e.g. that the path could be retrieved from _out etc.
                  im => {
                    val parsed: (ByteStringBuilder, ExtraData) = totals.parsed(im.meta.path)
                    val enrichResult = ByteString(parsed._2.getQuads(im.meta.indexTime, dcKey.modifier))
                    (InfotonData(im.meta, parsed._1.result ++ enrichResult), parsed._2)
                  }
                }(breakOut))
                (parsedResult, state, Option(totals.unParsed.result))
              }
              .withAttributes(ActorAttributes.supervisionStrategy(decider))
              .recover {
                case ex: Throwable =>
                  (Failure[RetrieveOutput](ex), (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))), None)
              }
          }
          case (res@Success(HttpResponse(s, h, entity, _)), state) => {
            val errorID = res.##
            val e = new Exception(
              s"Error ![$errorID]. Cm-Well returned bad response: status: ${s.intValue} headers: ${
                Util
                  .headersString(h)
              } reason: ${s.reason}."
            )
            val bodyFut =
              entity.dataBytes.runFold(empty)(_ ++ _).map(_.utf8String)
            val ex = RetrieveBadResponseException(
              s"Retrieve infotons failed. Sync $dcKey uuids: ${
                state._1
                  .map(i => i.meta.uuid.utf8String)
                  .mkString(",")
              }.",
              bodyFut,
              e
            )
            //            logger.warn("Retrieve infotons failed.", ex)
            Source.single(
              Failure[RetrieveOutput](ex),
              (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))),
              None
            )
          }
          case (Failure(e), state) => {
            val ex = RetrieveException(
              s"Retrieve infotons failed. Sync $dcKey uuids: ${
                state._1
                  .map(i => i.meta.uuid.utf8String)
                  .mkString(",")
              }",
              e
            )
            //              logger.warn("Retrieve infotons failed.", ex)
            Source.single(
              Failure[RetrieveOutput](ex),
              (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))),
              None
            )
          }
        }
        .map(checkResponse)

    Flow[RetrieveInput]
      .map(input => Future.successful(input) -> (input -> initialRetrieveBulkStatus))
      .via(Retry.concat(Settings.retrieveRetryQueueSize, retrieveFlow)(retryDecider(dcKey)))
      .map {
        case (Success(data), _) => data.map(_._1)
        case (Failure(e), _) =>
          logger.error(s"Sync $dcKey. Retrieve should never fail after retry.", e)
          throw e
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
      val extraData = {
        //Start with looking for indexTime
        val pos = line.indexOf("/meta/sys#indexTime") + 22
        //-1 + 22 == 21 - no match is found
        if (pos != 21) {
          val untilPos = line.indexOf('"', pos)
          ExtraData (uri, Some(line.substring(pos, untilPos).toLong), None)
        }
        else {
          //look for lastModifiedBy
          val pos = line.indexOf("/meta/sys#lastModifiedBy") + 27
          //-1 + 27 == 26 - no match is found
          if (pos != 26) {
            val untilPos = line.indexOf('"', pos)
            ExtraData (uri, None, Some(line.substring(pos, untilPos)))
          }
          else {
            ExtraData (uri, None, None)
          }
        }
      }

      val nquad = ByteString(line)
      Some(ParsedNquad(path, nquad, extraData))
    } else None
  }

  val gzipAcceptEncoding = `Accept-Encoding`(HttpEncodings.gzip)
  val gzipContentEncoding = `Content-Encoding`(HttpEncodings.gzip)
  def createRequest(dst: String, payload: ByteString) = {
    val entity =
      HttpEntity(ContentTypes.`text/plain(UTF-8)`, Gzip.encode(payload))
    HttpRequest(
      method = HttpMethods.POST,
      uri = s"$dst/_out?format=nquads&raw",
      entity = entity,
      headers = scala.collection.immutable.Seq(gzipAcceptEncoding, gzipContentEncoding)
    )
  }

  def checkResponseCreator(dcKey: DcInfoKey, decider: Decider)(
    response: (Try[RetrieveOutput], RetrieveState, Option[ByteString])
  ): (Try[RetrieveOutput], RetrieveState) =
    response match {
      case (response @ Success(res), state @ (input, status), body) => {
        val missingUuids = res.collect {
          case (id, _) if id.data == empty => id
        }
        //uuids that the index time from cassandra is different from the one got from ES
        val uuidsWithBadIndexTime = res.collect {
          case (id, extraData) if extraData.indexTime.isDefined && id.meta.indexTime != extraData.indexTime.get =>
            id
        }
        if (missingUuids.isEmpty && uuidsWithBadIndexTime.isEmpty) {
          status.lastException.foreach { e =>
            val bulkCount =
              if (input.size > 1)
                Settings.initialBulkRetrieveRetryCount - status.retriesLeft + 1
              else Settings.initialBulkRetrieveRetryCount + 1
            val singleCount =
              if (input.size > 1) 0
              else
                Settings.initialSingleRetrieveRetryCount - status.retriesLeft + 1
            yellowlog.info(
              s"Retrieve succeeded only after $bulkCount bulk retrieves and $singleCount single infoton retrieves. uuids: ${input
                .map(i => i.meta.uuid.utf8String)
                .mkString(",")}."
            )
          }
          (response, state)
        } else if (missingUuids.nonEmpty) {
          val errorID = response.##
          val u = missingUuids.map(i => i.meta.uuid.utf8String).mkString(",")
          val ex = RetrieveMissingUuidException(s"Error ![$errorID]. Retrieve infotons failed. Some infotons don't have data. " +
                                                s"Sync $dcKey missing uuids: $u.")
          logger.debug(s"Error ![$errorID]. Full response body was: $body")
          (Failure(ex), (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))))
        } else {
          val errorID = response.##
          val u = uuidsWithBadIndexTime.map(i => i.meta.uuid.utf8String).mkString(",")
          val ex = RetrieveBadIndexTimeException(s"Error ![$errorID]. Retrieve infotons failed. Some infotons' indexTime has cas/es inconsistencies. " +
                                                 s"Sync $dcKey bad uuids: $u.")
          logger.debug(s"Error ![$errorID]. Full response body was: $body")
          (Failure(ex), (state._1, RetrieveStateStatus(state._2.retriesLeft, Some(ex))))
        }
      }
      case (fail @ Failure(e), state, _) => (fail, state)
    }

  def retryDecider(dcKey: DcInfoKey)(implicit sys: ActorSystem, mat: Materializer) =
    (state: RetrieveState) =>
      state match {
        case (ingestSeq, RetrieveStateStatus(retriesLeft, ex)) =>
          if (ingestSeq.size == 1 && retriesLeft == 0) {
            ex.get match {
              case e: FuturedBodyException =>
                logger.error(s"${e.getMessage} ${e.getCause.getMessage} No more retries will be done. Please use the red " +
                             s"log to see the list of all the failed retrievals.")
                Util.errorPrintFuturedBodyException(e)
                redlog.info(s"Sync $dcKey: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} of " +
                            s"path ${ingestSeq.head.meta.path} failed.")
              case e: RetrieveMissingUuidException =>
                logger.error(s"Sync $dcKey: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} " +
                             s"failed. No more reties will be done. Please use the red log to see the list " +
                             s"of all the failed retrievals. The exception is:\n${e.getMessage}")
                redlog.info(s"Sync $dcKey: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} of " +
                            s"path ${ingestSeq.head.meta.path} failed. No uuid got from _out.")
              case e: RetrieveBadIndexTimeException =>
                logger.error(s"Sync $dcKey: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} " +
                             s"failed. No more reties will be done. Please use the red log to see the list of all " +
                             s"the failed retrievals. The exception is:\n${e.getMessage}")
                redlog.info(s"Sync $dcKey: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} of " +
                            s"path ${ingestSeq.head.meta.path} failed. IndexTime has cas/es inconsistency.")
              case e =>
                logger.error(s"Sync $dcKey: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} failed. " +
                             s"No more reties will be done. Please use the red log to see the list of all the failed retrievals. The exception is: ", e)
                redlog.info(s"Sync $dcKey: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} of " +
                            s"path ${ingestSeq.head.meta.path} failed.")
            }
            Some(List.empty[(Future[RetrieveInput], RetrieveState)])
          } else if (ingestSeq.size == 1) {
            logger.trace(s"Sync $dcKey: Retrieve of uuid ${ingestSeq.head.meta.uuid.utf8String} failed. " +
                         s"Retries left $retriesLeft. Will try again. The exception is: ", ex.get)
            Util.tracePrintFuturedBodyException(ex.get)
            val ingestState =
              (ingestSeq, RetrieveStateStatus(retriesLeft - 1, ex))
            val delay = Settings.maxSingleRetrieveRetryDelay.div(retriesLeft)
            Some(
              List(
                akka.pattern.after(delay, sys.scheduler)(
                  Future.successful(ingestSeq)
                ) -> ingestState
              )
            )
          } else if (retriesLeft == 0) {
            logger.trace(s"Sync $dcKey: Retrieve of bulk uuids failed. No more bulk retries left. " +
                         s"Will split to request for each uuid and try again. The exception is: ", ex.get)
            Util.tracePrintFuturedBodyException(ex.get)
            Some(ingestSeq.map { infotonMetaAndData =>
              val ingestData = Seq(infotonMetaAndData)
              val ingestState = ingestData -> RetrieveStateStatus(
                Settings.initialSingleRetrieveRetryCount,
                ex
              )
              val delay = Settings.maxSingleRetrieveRetryDelay.div(
                Settings.initialSingleRetrieveRetryCount
              )
              akka.pattern.after(delay, sys.scheduler)(
                Future.successful(ingestData)
              ) -> ingestState
            }(breakOut2))
          } else {
            logger.trace(s"Sync $dcKey: Retrieve of bulk uuids failed. Retries left $retriesLeft. Will try again. The exception is: ", ex.get)
            Util.tracePrintFuturedBodyException(ex.get)
            val ingestState =
              (ingestSeq, RetrieveStateStatus(retriesLeft - 1, ex))
            val delay = Settings.maxBulkRetrieveRetryDelay.div(retriesLeft)
            Some(
              List(
                akka.pattern.after(delay, sys.scheduler)(
                  Future.successful(ingestSeq)
                ) -> ingestState
              )
            )
          }
    }
}
