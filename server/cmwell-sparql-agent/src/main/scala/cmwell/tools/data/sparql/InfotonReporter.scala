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
package cmwell.tools.data.sparql

import akka.actor.{Actor, ActorRef, Props}
import akka.stream.{ActorMaterializer, Materializer}
import akka.pattern._
import akka.stream.scaladsl.{Sink, Source}
import cmwell.tools.data.downloader.consumer.Downloader.Token
import cmwell.tools.data.sparql.InfotonReporter._
import cmwell.tools.data.sparql.StpUtil.extractLastPart
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.HttpAddress
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.zstore.ZStore
import com.typesafe.config.ConfigFactory
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object InfotonReporter {

  case object RequestDownloadStats
  case object RequestIngestStats

  case class ResponseDownloadStats(stats: Map[String, DownloadStats])
  case class ResponseIngestStats(stats: Map[String, IngestStats])

  def apply(baseUrl: String, path: String, zStore: ZStore)(implicit mat: Materializer, ec: ExecutionContext) =
    Props(new InfotonReporter(baseUrl, path, zStore))
}

class InfotonReporter private (baseUrl: String, path: String, zStore: ZStore)(implicit mat: Materializer, ec: ExecutionContext)
    extends Actor
    with SparqlTriggerProcessorReporter
    with DataToolsLogging {
  if (mat.isInstanceOf[ActorMaterializer]) {
    require(mat.asInstanceOf[ActorMaterializer].system eq context.system,
            "ActorSystem of materializer MUST be the same as the one used to create current actor")
  }
  val HttpAddress(protocol, host, port, _) = ArgsManipulations.extractBaseUrl(baseUrl)
  val format = "ntriples"
  val writeToken = ConfigFactory.load().getString("cmwell.agents.sparql-triggered-processor.write-token")
  var downloadStats: Map[String, DownloadStats] = Map()
  var ingestStats: Map[String, IngestStats] = Map()

  val name = StpUtil.extractLastPart(path)

  override def preStart(): Unit = StpUtil.readPreviousTokens(baseUrl, path, zStore = zStore).onComplete(self ! _)

  override val receive: Receive = receiveBeforeInitializes(Nil) //receiveWithMap(Map.empty)

  def receiveBeforeInitializes(recipients: List[ActorRef]): Receive = {
    case RequestPreviousTokens =>
      context.become(receiveBeforeInitializes(sender() :: recipients))

    case Success(savedTokens: TokenAndStatisticsMap) =>
      context.become(receiveWithMap(savedTokens))
      recipients.foreach(_ ! ResponseWithPreviousTokens(Right(savedTokens)))

    case Failure(ex) =>
      logger.error("cannot read previous tokens infoton")
      context.become(receiveWithMap(Map.empty))
      recipients.foreach(_ ! ResponseWithPreviousTokens(Left(ex.getMessage)))

    case s: DownloadStats =>
      downloadStats += (s.label.getOrElse("") -> s)

    case ingest: IngestStats =>
      ingestStats += (ingest.label.getOrElse("") -> ingest)

    case RequestDownloadStats =>
      sender() ! ResponseDownloadStats(downloadStats)

    case RequestIngestStats =>
      sender() ! ResponseIngestStats(ingestStats)

    case RequestReference(path) =>
      val data = getReferencedData(path)
      data.map(ResponseReference.apply).pipeTo(sender())
  }

  def receiveWithMap(tokensAndStats: TokenAndStatisticsMap): Receive = {
    case RequestPreviousTokens =>
      sender() ! ResponseWithPreviousTokens(Right(tokensAndStats))

    case ReportNewToken(sensor, token) =>
      val updatedTokens = tokensAndStats + (sensor -> (token, downloadStats.get(sensor)))
      saveTokens(updatedTokens)
      context.become(receiveWithMap(updatedTokens))

    case s: DownloadStats =>
      downloadStats += (s.label.getOrElse("") -> s)

    case s: IngestStats =>
      ingestStats += (s.label.getOrElse("") -> s)

    case RequestDownloadStats =>
      sender() ! ResponseDownloadStats(downloadStats)

    case RequestIngestStats =>
      sender() ! ResponseIngestStats(ingestStats)

    case RequestReference(path) =>
      val data = getReferencedData(path)
      data.map(ResponseReference.apply).pipeTo(sender())
  }

  override def getReferencedData(path: String): Future[String] = {
    import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
    cmwell.util.http.SimpleHttpClient
      .get(s"http://$baseUrl$path")
      .map(_.payload)
  }

  def saveTokens(tokenAndStatistics: TokenAndStatisticsMap) : Unit = {

    def createJson(sensor: String, token: Token, downloadStats: Option[DownloadStats]) = {
      Json.fromFields({
        downloadStats match {
          case None => List(("sensor", Json.fromString(sensor)), ("token", Json.fromString(token)))
          case Some(downloadStats) =>
            List(("sensor", Json.fromString(sensor)), ("token", Json.fromString(token)),
              ("receivedInfotons", Json.fromLong(downloadStats.receivedInfotons)))
        }
      }).noSpaces
    }

    def createTokenPayload(tokenAndStatistics: TokenAndStatisticsMap)  = {
      tokenAndStatistics.foldLeft(Seq.empty[String]) {
        case (agg, (sensor, (token, downloadStats))) => agg :+ createJson(sensor, token, downloadStats)
      }.mkString("\n")
    }

    Source
      .single(tokenAndStatistics)
      .map{createTokenPayload}
      .map{
        zStore.putString(s"stp-agent-${extractLastPart(path)}", _)
      }
      .map(zStoreFuture => {
        zStoreFuture.onComplete {
          case Success(_) => logger.debug(s"successfully written tokens to zStore, key=$path")
          case Failure(ex) => logger.error(s"problem writing tokens to zStore: ${ex.getMessage}")
        }
      })
      .runWith(Sink.ignore)
  }
}
