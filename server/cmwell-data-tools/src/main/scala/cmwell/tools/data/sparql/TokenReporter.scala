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

import java.nio.file.{Files, Paths}

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.pattern._
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, Materializer}
import cmwell.tools.data.downloader.consumer.Downloader.Token
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats
import cmwell.tools.data.utils.logging.DataToolsLogging

import scala.concurrent.{ExecutionContext, Future}

trait SparqlTriggerProcessorReporter {

  /**
    * Reads a referenced data from sparql-triggered-processor config file.
    * Referenced data is a path starts with '@' character
    *
    * @param path path containing referenced data (e.g., '@foo/bar/baz.txt')
    * @return referenced data
    */
  def getReferencedData(path: String): Future[String]

  /**
    * Store given tokens for a future usage (e.g., in a non-volatile memory)
    * @param tokensAndStats tokens with current statistics to be saved
    */
  def saveTokens(tokensAndStats: TokenAndStatisticsMap): Unit
}

/**
  * Reporter which reads/writes token and stats to files
  * @param stateFile path to file which stores current state (tokens)
  * @param webPort
  */
class FileReporterActor(stateFile: Option[String], webPort: Int = 8080)
    extends Actor
    with SparqlTriggerProcessorReporter
    with DataToolsLogging {
  val path = stateFile.map(Paths.get(_))
  implicit val ec = context.dispatcher

  val webReporter =
    new WebExporter(self, webPort)(context.system, ActorMaterializer())

  override val receive: Receive = receiveWithMap(readTokensFromFile())

  def receiveWithMap(tokens: Map[String, Token]): Receive = {
    case RequestPreviousTokens =>
      sender() ! ResponseWithPreviousTokens(tokens.map {
        case (sensor, token) => sensor -> (token, None)
      })
    case ReportNewToken(sensor, token) =>
      val updatedTokens = tokens + (sensor -> token)
      saveTokens(updatedTokens.map {
        case (sensor, token) => (sensor -> (token, None))
      })

      context.become(receiveWithMap(updatedTokens))
    case RequestReference(path) =>
      val data = getReferencedData(path)
      //      sender() ! ResponseReference(data)
      data.map(ResponseReference.apply).pipeTo(sender())
  }

  def readTokensFromFile() =
    path
      .map { p =>
        if (!Files.exists(p)) Map.empty[String, Token]
        else
          scala.io.Source
            .fromFile(p.toFile)
            .getLines()
            .map(line => line.split(" -> "))
            .map(arr => arr(0) -> arr(1))
            .toMap
      }
      .getOrElse(Map.empty)

  override def getReferencedData(path: String): Future[String] =
    Future.successful(scala.io.Source.fromFile(path).mkString)

  override def saveTokens(tokensAndStats: TokenAndStatisticsMap): Unit = {
    val tokens = tokensAndStats.map {
      case (sensor, (token, _)) => sensor -> token
    }
    path.foreach(p => Files.write(p, tokens.mkString("\n").getBytes("UTF-8")))
  }
}

class WebExporter(reporter: ActorRef, port: Int = 8080)(
  implicit system: ActorSystem,
  mat: Materializer
) {

  implicit val ec = system.dispatcher

  val serverSource = Http().bind(interface = "localhost", port = port)

  val requestHandler: HttpRequest => Future[HttpResponse] = {
    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      createContent.map { content =>
        HttpResponse(
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, content)
        )
      }
  }

  val bindingFuture: Future[Http.ServerBinding] =
    serverSource
      .to(Sink.foreach { connection =>
        connection.handleWithAsyncHandler(requestHandler)
      })
      .run()

  def createContent(implicit ec: ExecutionContext) = {
    import scala.concurrent.duration._
    implicit val timeout = akka.util.Timeout(10.seconds)
    (reporter ? RequestPreviousTokens)
      .mapTo[ResponseWithPreviousTokens]
      .map {
        case ResponseWithPreviousTokens(tokens) =>
          val title = "sensors state"

          val (content, _) = tokens.foldLeft("" -> false) {
            case ((agg, evenRow), (sensor, token)) =>
              val style = if (evenRow) "tg-j2zy" else "tg-yw4l"

              val decoded =
                cmwell.tools.data.utils.text.Tokens.decompress(token._1)
              val timestamp = DateTime(decoded.takeWhile(_ != '|').toLong)

              val row =
                s"""
              |<tr>
              |    <td class="$style">$sensor</th>
              |    <td class="$style">$timestamp</th>
              |    <td class="$style">$decoded</th>
              |    <td class="$style">$token</th>
              | </tr>
            """.stripMargin

              (agg ++ "\n" ++ row) -> !evenRow
          }

          s"""
          |<html><body>
          |<style type="text/css">
          |.tg  {border-collapse:collapse;border-spacing:0;border-color:#aaa;}
          |.tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#aaa;color:#333;background-color:#fff;}
          |.tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#aaa;color:#fff;background-color:#f38630;}
          |.tg .tg-j2zy{background-color:#FCFBE3;vertical-align:top}
          |.tg .tg-yw4l{vertical-align:top}
          |</style>
          |<table class="tg">
          |  <tr>
          |    <th class="tg-yw4l">sensor</th>
          |    <th class="tg-yw4l">point in time</th>
          |    <th class="tg-yw4l">decoded token</th>
          |    <th class="tg-yw4l">raw token</th>
          |  </tr>
          |  $content
          |</table>
          |</body></html>
        """.stripMargin

      }
  }
}

case object RequestPreviousTokens

case class ResponseWithPreviousTokens(tokens: TokenAndStatisticsMap)
case class ReportNewToken(sensor: String, token: Token)
case class RequestReference(path: String)
case class ResponseReference(data: String)
