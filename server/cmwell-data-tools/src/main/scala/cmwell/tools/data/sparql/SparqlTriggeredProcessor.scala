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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern._
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Source}
import akka.stream.{Materializer, SourceShape}
import akka.util.ByteString
import cmwell.tools.data.downloader.consumer.{Downloader => Consumer}
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Tokens

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

case class Config(name: Option[String] = None,
                  sensors: Seq[Sensor],
                  updateFreq: FiniteDuration,
                  sparqlMaterializer: String,
                  hostUpdatesSource: Option[String],
                  force: Option[Boolean] = Some(false),
                  useQuadsInSp: Option[Boolean])

object SparqlTriggeredProcessor extends DataToolsLogging {

  val sparqlMaterializerLabel = "sparql-materializer"

  val stpSpQueryBuilder = (path: Seq[String], vars: Map[String, String], useQuads: Boolean) => {
    (if (path.head.length > 0) {
      "sp.pid=" + path.head.substring(path.head.lastIndexOf('-') + 1) +
        "&sp.path=" + path.head.substring(path.head.lastIndexOf('/') + 1)
    }
    else {
      ""
    }) +
      vars.foldLeft("") {
        case (string, (key, value)) => {
          string + "sp." + key + "=" + value + "&"
        }
      } + (
      useQuads match {
        case true => "&quads"
        case false => ""
      })
  }

  def downloadStatsFlow(tokenReporterOpt: Option[ActorRef], format: String, sensorName: String,  initialDownloadStatsOpt: Option[DownloadStats] = None) = {

    import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats._

    type DownloadElement = (ByteString, Option[SensorContext])

    val emptyElement: DownloadElement = (ByteString.empty , None)

    def markStats(elem: DownloadElement, downloadStats: DownloadStats) = elem match {
      case (_, Some(SensorContext(_, _, horizon, remainingInfotons))) =>
        DownloadStats(receivedInfotons = downloadStats.receivedInfotons + countInfotonsInChunk(elem, format),
          statsTime = System.currentTimeMillis(),
          receivedBytes = downloadStats.receivedBytes + bytesRead(elem),
          horizon = horizon,
          remaining = remainingInfotons,
          label = downloadStats.label)
      case (elem, None) => logger.error(s"downloadStats is None for element: $elem"); ???
    }

    Flow[DownloadElement]
      .scan(initialStats(initialDownloadStatsOpt, sensorName, emptyElement))((stats, elem) =>
        (markStats(elem, stats._1), elem))
      .groupedWithin(100,3.seconds)
      .map( collatedPaths => {
        collatedPaths.last match {
          case (downloadStats, _) =>
            tokenReporterOpt.foreach { _ ! downloadStats }
          }
          collatedPaths
        }
      )
      .mapConcat(_.map(_._2))
  }

  def preProcessConfig(config: Config, tokenReporter: Option[ActorRef])(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    def getReferencedData(path: String) = tokenReporter match {
      case None => Future.successful("")
      case Some(reporter) =>
        implicit val timeout = akka.util.Timeout(30.seconds)
        (reporter ? RequestReference(path.tail))
          .mapTo[ResponseReference]
          .map { case ResponseReference(data) => data }
    }

    val configWithProcessedMaterializer = if (config.sparqlMaterializer.startsWith("@")) {
      getReferencedData(config.sparqlMaterializer)
        .map(data => config.copy(sparqlMaterializer = data))
    } else {
      Future.successful(config)
    }

    configWithProcessedMaterializer.flatMap { c =>
      val processedSensors = c.sensors.map {
        case sensor@Sensor(_, _, _, _, _, Some(sparqlToRoot)) if sparqlToRoot.startsWith("@") =>
          getReferencedData(sparqlToRoot).map(data => sensor.copy(sparqlToRoot = Some(data)))
        case sensor =>
          Future.successful(sensor)
      }

      Future
        .sequence(processedSensors)
        .map(updatedSensors => c.copy(sensors = updatedSensors))
    }
  }

  def createSensorSource(config: Config,
                         initialTokensAndStatistics: Either[String,AgentTokensAndStatistics],
                         tokenReporter: Option[ActorRef] = None,
                         baseUrl: String,
                         isBulk: Boolean = false,
                         distinctWindowSize: FiniteDuration,
                         infotonGroupSize: Integer
                        )
                        (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    initialTokensAndStatistics match {

      case Left(error) =>
        Source.failed(new Exception(error))

      case Right(tokensAndStatistics) => {

        var savedTokens = tokensAndStatistics.sensors.map {
          case (sensor, (token, _)) => sensor -> token
        }

        Source.fromGraph(GraphDSL.create() { implicit builder =>
          import GraphDSL.Implicits._

          val merger = builder.add(Merge[((ByteString, Map[String, String]), Option[SensorContext])](config.sensors.size))

          for ((sensor, i) <- config.sensors.zipWithIndex) {
            // if saved token available, ignore token in configuration
            // if configuration token was provided, ignore index-time
            val savedToken = savedTokens.get(sensor.name)
            val tokenFuture = if (savedToken.isDefined) {
              logger.debug("received previous value of sensor {}: {}", sensor.name, savedToken.get)
              Future.successful(savedToken)
            } else
              sensor.token match {
                case None =>
                  Consumer
                    .getToken(baseUrl = baseUrl,
                      path = sensor.path,
                      qp = sensor.qp,
                      isBulk = isBulk,
                      indexTime = sensor.fromIndexTime)
                    .map(Option.apply)

                case token => Future.successful(token)
              }

            // get updates from sensor

            val source = Source
              .fromFuture(tokenFuture)
              .flatMapConcat {
                token =>
                  Consumer.createTsvSource(
                      baseUrl = baseUrl,
                      path = sensor.path,
                      qp = sensor.qp,
                      //            params  = params,
                      isBulk = isBulk,
                      token = token,
                      updateFreq = Some(config.updateFreq),
                      label = Some(sensor.name)
                    )
                    .map {
                      case ((token, tsv), hz, remaining) =>
                        val path = tsv.path
                        logger.debug("sensor [{}] found new path: {}", sensor.name, path.utf8String)
                        path -> Some(SensorContext(name = sensor.name, token = token, horizon = hz, remainingInfotons = remaining))

                      case x =>
                        logger.error(s"unexpected message: $x")
                        ???
                    }.via(
                      downloadStatsFlow(tokenReporterOpt=tokenReporter,
                        format="ntriples",
                        sensorName = sensor.name,
                        initialDownloadStatsOpt = {
                          for {
                            sensor <- tokensAndStatistics.sensors.get(sensor.name)
                            initial <- sensor._2
                          } yield initial
                        })
                    )
              } .map(source => (source._1, Map.empty[String, String]) -> source._2)


            val pathSource = if (sensor.sparqlToRoot.isDefined) {

              source.map {
                case ((path, _), context) =>
                  context.foreach(
                    c => logger.debug("sensor [{}] is trying to get root infoton of {}", c.name, path.utf8String)
                  )
                  (path, Map.empty[String, String]) -> context
              }
              .via(
                SparqlProcessor.sparqlSourceFromPathsFlow(
                  baseUrl = baseUrl,
                  isNeedWrapping = false,
                  sparqlQuery = sensor.sparqlToRoot.get,
                  spQueryParamsBuilder = (p: Seq[String], v: Map[String, String], q: Boolean) => "sp.pid=" + p.head.substring(p.head.lastIndexOf('-') + 1),
                  format = Some("tsv"),
                  label = Some(sensor.name)
                )
              )
              .filter { case (data, _) => data.startsWith("?") }
              .map {
                case (data, sensorContext) =>

                  val vars = data.utf8String.filterNot("?\"".toSet)
                    .split("\n")
                    .map {
                      _.split("\t")
                    }
                    .transpose
                    .map { f =>
                      f(0) -> {
                        if (f.isDefinedAt(1)) f(1) else ""
                      }
                    }
                    .toMap

                  val path =
                    if (vars.size == 1) {
                      ByteString(vars.head._2.drop(8).dropRight(1))
                    }
                    else {
                      ByteString("")
                    }

                  (path, vars) -> sensorContext
              }
              .filter { case ((path, vars), _) => path.nonEmpty || vars.nonEmpty }

            } else {
              source
            }

            pathSource ~> merger.in(i)
          }

          SourceShape(merger.out) // todo: might change this to UniformFanInShape
        })
        .groupedWithin(infotonGroupSize, distinctWindowSize)
        .statefulMapConcat {
          () =>
            // stores last received tokens from sensors
            sensorData => {
              sensorData.foreach {
                case (_, Some(SensorContext(name, newToken, _, _))) if newToken != savedTokens.getOrElse(name, "") =>
                  // received new token from sensor, write it to state file
                  logger.debug("sensor '{}' received new token: {} {}", name, Tokens.decompress(newToken), newToken)
                  tokenReporter.foreach(_ ! ReportNewToken(name, newToken))

                  //            stateFilePath.foreach { path => Files.write(path, savedTokens.mkString("\n").getBytes("UTF-8")) }
                  savedTokens = savedTokens + (name -> newToken)
                case _ =>
              }

              sensorData
                .map { case (data, _) => data }
                .distinct
                .map(_ -> None)
            }
        }
        .map {
          case (path, _) =>
            logger.debug("request materialization of {}", path._1.utf8String)
            path -> None
          case x =>
            logger.error(s"unexpected message: $x")
            (ByteString(""), Map.empty[String, String]) -> None
        }
      }
    }
  }

  def loadInitialTokensAndStatistics(tokenReporter : Option[ActorRef])
                                    (implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = tokenReporter match {

    case None => Right(AgentTokensAndStatistics(Map.empty[String, TokenAndStatistics], None, None))
    case Some(reporter) =>
      import akka.pattern._
      implicit val t = akka.util.Timeout(1.minute)
      val result = (reporter ? RequestPreviousTokens)
        .mapTo[ResponseWithPreviousTokens]
        .map {
          case ResponseWithPreviousTokens(tokens) => tokens
          case x => logger.error(s"did not receive previous tokens: $x"); Left(s"did not receive previous tokens: $x")
        }
      Await.result(result, 1.minute)
  }
}

