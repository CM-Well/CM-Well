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
package cmwell.dc.stream

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model.headers.{HttpEncodings, RawHeader, `Accept-Encoding`, `Content-Encoding`}
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream.Supervision._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream._
import akka.util.{ByteString, ByteStringBuilder}
import cmwell.dc.Settings.config
import cmwell.dc.{LazyLogging, Settings, stream}
import cmwell.dc.stream.MessagesTypesAndExceptions._
import cmwell.dc.stream.SingleMachineInfotonIngester.IngestInput
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by eli on 27/06/16.
  */
object SingleMachineInfotonIngester extends LazyLogging {

  type IngestInput = Seq[BaseInfotonData]
  type IngestOutput = HttpResponse

  case class IngestStateStatus(retriesLeft: Int, singleRetryCount: Int, lastException: Option[Throwable])

  val okStatus = IngestStateStatus(0, 0, None)
  private val dcaToken = Settings.dcaUserToken
  type IngestState = (IngestInput, IngestStateStatus)

  def apply(dcKey: DcInfoKey, location: String, decider: Decider)(
    implicit sys: ActorSystem,
    mat: Materializer
  ): Flow[(Future[IngestInput], IngestState), (Try[IngestOutput], IngestState), NotUsed] = {
    val checkResponse = checkResponseCreator(dcKey, location, decider) _
    val poolConfig = ConfigFactory
      .parseString("akka.http.host-connection-pool.max-connections=1")
      .withFallback(config)
    val (host, port) = location.split(':') match {
      case Array(host) => host -> 80
      case Array(host, port) => host -> port.toInt
    }
    val connPool = Http()
      .newHostConnectionPool[IngestState](
        host,
        port,
        ConnectionPoolSettings(poolConfig)
      )
    Flow[(Future[IngestInput], IngestState)]
      .mapAsync(1) { case (input, state) => input.map(_ -> state) }
      .map {
        case (infotonSeq, state) => {
          val payloadBuilder = new ByteStringBuilder
          // no need for end line because each line in already suffixed with it
          infotonSeq.foreach{i=>
            val firstLine = i.data.takeWhile(_ != stream.newLine)
            if(firstLine.utf8String.contains("meta/sys#uuid"))
              payloadBuilder ++= i.data.drop(firstLine.length + 1)
            else
              payloadBuilder ++= i.data
          }
          val payload = payloadBuilder.result
          (createRequest(location, payload, dcKey.ingestOperation), state)
        }
      }
      .via(connPool)
      .map(checkResponse)
  }

  private[this] val createRequest = if (Settings.gzippedIngest) createRequestWithGzip _ else createRequestNoGzip _
  private val tokenHeader: HttpHeader = RawHeader("X-CM-WELL-TOKEN", dcaToken)

  private[this] def createRequestNoGzip(location: String, payload: ByteString, op:String) = {
    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, payload)
    HttpRequest(method = HttpMethods.POST,
                uri = s"http://$location/$op?format=nquads",
                entity = entity,
                headers = scala.collection.immutable.Seq(tokenHeader))
  }

  val gzipContentEncoding = `Content-Encoding`(HttpEncodings.gzip)
  private[this] def createRequestWithGzip(location: String, payload: ByteString, op:String) = {
    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, Gzip.encode(payload))
    HttpRequest(
      method = HttpMethods.POST,
      uri = s"http://$location/$op?format=nquads",
      entity = entity,
      headers = scala.collection.immutable.Seq(tokenHeader, gzipContentEncoding)
    )
  }

  def checkResponseCreator(dcKey: DcInfoKey, location: String, decider: Decider)(
    response: (Try[HttpResponse], IngestState)
  )(implicit sys: ActorSystem, mat: Materializer): (Try[HttpResponse], IngestState) =
    response match {
      case (Success(res), state @ (input, status)) if res.status.isSuccess() => {
        // while both of the following issues are still open:
        // https://github.com/akka/akka/issues/18540
        // https://github.com/akka/akka/issues/18716
        // we must consume the empty entity body
        res.entity.dataBytes.withAttributes(ActorAttributes.supervisionStrategy(decider)).runWith(Sink.ignore)
        //the Option with the exception will be only if ingest has already failed at least once.
        status.lastException.foreach { e =>
          val bulkCount =
            if (input.size > 1) Settings.initialBulkIngestRetryCount - status.retriesLeft + 1
            else Settings.initialBulkIngestRetryCount + 1
          val singleCount = status.singleRetryCount + 1 //if (state._1.size > 1) 0 else Settings.initialSingleIngestRetryCount - state._2.retriesLeft + 1
          yellowlog.info(
            s"Ingest succeeded only after $bulkCount bulk ingests and $singleCount single infoton ingests. uuids: ${input
              .map(i => Util.extractUuid(i))
              .mkString(",")}."
          )
        }
        (response._1, (state._1, okStatus))
      }
      case (res @ Success(HttpResponse(s, _, entity, _)), state) => {
        val errorID = res.##
        val e = new Exception(
          s"Error ![$errorID]. Cm-Well returned bad response: status: ${s.intValue} reason: ${s.reason}"
        )
        val bodyFut = entity.dataBytes.runFold(empty)(_ ++ _).map(_.utf8String)
        val ex =
          if (s == StatusCodes.ServiceUnavailable)
            IngestServiceUnavailableException(
              s"Ingest infotons failed. Sync $dcKey, using local location $location uuids: ${state._1
                .map(i => Util.extractUuid(i))
                .mkString(",")}.",
              bodyFut,
              e
            )
          else
            IngestBadResponseException(
              s"Ingest infotons failed. Sync $dcKey, using local location $location uuids: ${state._1
                .map(i => Util.extractUuid(i))
                .mkString(",")}.",
              bodyFut,
              e
            )
        //        logger.warn("Ingest infotons failed.", ex)
        (Failure[HttpResponse](ex),
         (state._1, IngestStateStatus(state._2.retriesLeft, state._2.singleRetryCount, Some(ex))))
      }
      case (Failure(e), state) => {
        val ex = IngestException(
          s"Ingest infotons failed. Sync $dcKey, using local location $location uuids: ${state._1
            .map(i => Util.extractUuid(i))
            .mkString(",")}",
          e
        )
        //        logger.warn("Ingest infotons failed.", ex)
        (Failure[HttpResponse](ex),
         (state._1, IngestStateStatus(state._2.retriesLeft, state._2.singleRetryCount, Some(ex))))
      }
    }


}
