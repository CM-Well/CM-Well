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
  case class ResponseIngestStats(stats: Option[IngestStats])

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
  var ingestStats: Option[IngestStats] = Some(IngestStats(None,0,0,0))

  val name = StpUtil.extractLastPart(path)

  override def preStart(): Unit = StpUtil.readPreviousTokens(baseUrl, path, zStore = zStore).onComplete(self ! _)

  override val receive: Receive = receiveBeforeInitializes(Nil) //receiveWithMap(Map.empty)

  def receiveBeforeInitializes(recipients: List[ActorRef]): Receive = {
    case RequestPreviousTokens =>
      context.become(receiveBeforeInitializes(sender() :: recipients))

    case Success(savedTokens: AgentTokensAndStatistics) =>
      context.become(receiveWithMap(savedTokens))
      recipients.foreach(_ ! ResponseWithPreviousTokens(Right(savedTokens)))

    case Failure(ex) =>
      logger.error("cannot read previous tokens infoton")
      context.become(receiveWithMap(AgentTokensAndStatistics(Map.empty)))
      recipients.foreach(_ ! ResponseWithPreviousTokens(Left(ex.getMessage)))

    case s: DownloadStats =>
      downloadStats += (s.label.getOrElse("") -> s)

    case s: IngestStats =>
      ingestStats = Some(s)

    case RequestDownloadStats =>
      sender() ! ResponseDownloadStats(downloadStats)

    case RequestIngestStats =>
      sender() ! ResponseIngestStats(ingestStats)

    case RequestReference(path) =>
      val data = getReferencedData(path)
      data.map(ResponseReference.apply).pipeTo(sender())
  }

  def receiveWithMap(tokensAndStats: AgentTokensAndStatistics): Receive = {
    case RequestPreviousTokens =>
      sender() ! ResponseWithPreviousTokens(Right(tokensAndStats))

    case ReportNewToken(sensor, token) =>
      val updatedTokens = tokensAndStats.sensors + (sensor -> (token, downloadStats.get(sensor)))
      val agentTokensAndStatistics = AgentTokensAndStatistics(updatedTokens,
        ingestStats,downloadStats.get(SparqlTriggeredProcessor.sparqlMaterializerLabel))

      saveTokens(agentTokensAndStatistics)
      context.become(receiveWithMap(agentTokensAndStatistics))

    case s: DownloadStats =>
      downloadStats += (s.label.getOrElse("") -> s)

    case s: IngestStats => ingestStats = Some(s)

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

  def saveTokens(tokenAndStatistics: AgentTokensAndStatistics) : Unit = {

    def createSensorJson(sensor: String, token: Token, downloadStats: Option[DownloadStats]) = {
      Json.fromFields({
        (downloadStats match {
          case None if !token.isEmpty => List(("sensor", Json.fromString(sensor)), ("token", Json.fromString(token)),
            ("position", Json.fromString( cmwell.tools.data.utils.text.Tokens.decompress(token))))
          case Some(downloadStats) =>
            List(("sensor", Json.fromString(sensor)), ("token", Json.fromString(token)),
              ("receivedInfotons", Json.fromLong(downloadStats.receivedInfotons)),
              ("position", Json.fromString( cmwell.tools.data.utils.text.Tokens.decompress(token))))
          case _ => List.empty
        })
      }).noSpaces
    }

    def createAgentJson(ingestStats: Option[IngestStats], materializedStats: Option[DownloadStats]) = {
      Seq(Json.fromFields( {
        (ingestStats match {
          case Some(ingestStats) => {
            List(("ingestedInfotons", Json.fromLong(ingestStats.ingestedInfotons)),
              ("failedInfotons", Json.fromLong(ingestStats.failedInfotons)))
          }
          case None => Nil
        }) ++
        (materializedStats match {
          case Some(materialized) => {
            List(("materializedInfotons",Json.fromLong(materialized.receivedInfotons)),
              ("totalRunningMillis", Json.fromLong(materialized.totalRunningTime)))
          }
          case None => Nil
        })
      }).noSpaces)
    }

    def createPayload(tokenAndStatistics: AgentTokensAndStatistics)  = {
      (tokenAndStatistics.sensors.collect({
        case downloadStats @(_,(_,Some(_))) => downloadStats
      }).foldLeft(Seq.empty[String]) {
        case (agg, (sensor, (token, Some(downloadStats)))) =>
          agg :+ createSensorJson(sensor, token, Some(downloadStats))
      } ++
        createAgentJson(tokenAndStatistics.agentIngestStats, tokenAndStatistics.materializedStats)).mkString("\n")
    }


    Source
      .single(tokenAndStatistics)
      .map{createPayload}
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
