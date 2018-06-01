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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern._
import akka.stream.scaladsl.{GraphDSL, Merge, Source}
import akka.stream.{Materializer, SourceShape}
import akka.util.ByteString
import cmwell.tools.data.downloader.consumer.{Downloader => Consumer}
import cmwell.tools.data.utils.akka.stats.DownloaderStats
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.text.Tokens

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

case class SensorContext(name: String, token: String, horizon: Boolean, remainingInfotons: Option[Long])

case class Sensor(name: String,
                  qp: String = "",
                  fromIndexTime: Long = 0,
                  path: String,
                  token: Option[String] = None,
                  sparqlToRoot: Option[String] = None) {

  override def toString: String = s"Sensor [name=$name, path=$path, qp=$qp, fromIndexTime=$fromIndexTime]"
}

case class Config(name: Option[String] = None,
                  sensors: Seq[Sensor],
                  updateFreq: FiniteDuration,
                  sparqlMaterializer: String,
                  hostUpdatesSource: Option[String],
                  force: Option[Boolean] = Some(false))

object SparqlTriggeredProcessor extends DataToolsLogging {

  val sparqlMaterializerLabel = "sparql-materializer"

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

  def listen(
              config: Config,
              baseUrl: String,
              isBulk: Boolean = false,
              tokenReporter: Option[ActorRef] = None,
              initialTokensAndStatistics: Either[String,AgentTokensAndStatistics] =
                Right(AgentTokensAndStatistics(Map.empty[String, TokenAndStatistics],None,None)),
              label: Option[String] = None,
              distinctWindowSize: FiniteDuration = 10.seconds,
              infotonGroupSize: Integer = 100
  )(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    new SparqlTriggeredProcessor(config = config,
                                 baseUrl = baseUrl,
                                 isBulk = isBulk,
                                 tokenReporter = tokenReporter,
                                 label = label,
                                 distinctWindowSize = distinctWindowSize,
                                 infotonGroupSize = infotonGroupSize)
      .listen(initialTokensAndStatistics)
  }
}

class SparqlTriggeredProcessor(config: Config,
                               baseUrl: String,
                               isBulk: Boolean = false,
                               tokenReporter: Option[ActorRef] = None,
                               override val label: Option[String] = None,
                               distinctWindowSize: FiniteDuration,
                               infotonGroupSize: Integer)
    extends DataToolsLogging {

  def listen(initialTokensAndStatistics: Either[String,AgentTokensAndStatistics])(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) = {

    def addStatsToSource(id: String,
                         source: Source[(ByteString, Option[SensorContext]), _],
                         initialDownloadStats: Option[DownloadStats] = None) = {
      source.via(
        DownloaderStats(
          format = "ntriples",
          label = Some(id),
          reporter = tokenReporter,
          initialDownloadStats = initialDownloadStats
        )
      )
    }

    def getReferencedData(path: String) = tokenReporter match {
      case None => Future.successful("")
      case Some(reporter) =>
        implicit val timeout = akka.util.Timeout(30.seconds)
        (reporter ? RequestReference(path.tail))
          .mapTo[ResponseReference]
          .map { case ResponseReference(data) => data }
    }

    def preProcessConfig(config: Config) = {
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

    initialTokensAndStatistics match {

      case Left(error) =>
        Source.failed(new Exception(error))

      case Right(tokensAndStatistics) => {

        var savedTokens = tokensAndStatistics.sensors.map {
          case (sensor, (token, _)) => sensor -> token
        }

        def createSensorSource(config: Config) = {
          Source.fromGraph(GraphDSL.create() { implicit builder =>
            import GraphDSL.Implicits._

            val merger = builder.add(Merge[(ByteString, Option[SensorContext])](config.sensors.size))

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
                    val tsvSource = Consumer
                      .createTsvSource(
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
                      }

                    addStatsToSource(id = sensor.name,
                      source = tsvSource,
                      initialDownloadStats = {for {
                        sensor <- tokensAndStatistics.sensors.get(sensor.name)
                        initial <- sensor._2
                      } yield initial}
                    )
                }

              // get root infoton
              val pathSource = if (sensor.sparqlToRoot.isDefined) {
                SparqlProcessor
                  .createSparqlSourceFromPaths(
                    baseUrl = baseUrl,
                    isNeedWrapping = false,
                    sparqlQuery = sensor.sparqlToRoot.get,
                    spQueryParamsBuilder = (p: Seq[String]) => "sp.pid=" + p.head.substring(p.head.lastIndexOf('-') + 1),
                    format = Some("tsv"),
                    label = Some(sensor.name),
                    source = source.map {
                      case (path, context) =>
                        context.foreach(
                          c => logger.debug("sensor [{}] is trying to get root infoton of {}", c.name, path.utf8String)
                        )
                        path -> context
                    }
                  )
                  .filter { case (data, _) => data.startsWith("?") }
                  .map {
                    case (data, sensorContext) =>
                      val path = data
                        .dropWhile(_ != '\n') // drop ?orgId\n
                        .drop(8)
                        .dropRight(1) // <http://data.thomsonreuters.com/1-34418459938>, drop <http:/, >

                      path -> sensorContext
                  }
                  .filter { case (path, _) => path.nonEmpty }

              } else {
                source
              }

              pathSource ~> merger.in(i)
            }

            SourceShape(merger.out) // todo: might change this to UniformFanInShape
          })
        }

        // populate unique changes on paths
        val processedConfig = Await.result(preProcessConfig(config), 3.minutes)

        val sensorSource = createSensorSource(processedConfig)
          .groupedWithin(infotonGroupSize, distinctWindowSize)
          .statefulMapConcat {
            () =>
              // stores last received tokens from sensors
              sensorData => {
                sensorData.foreach {
                  case (data, Some(SensorContext(name, newToken, _, _))) if newToken != savedTokens.getOrElse(name, "") =>
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
              logger.debug("request materialization of {}", path.utf8String)
              path -> None
            case x =>
              logger.error(s"unexpected message: $x")
              ByteString("") -> None
          }

        // execute sparql queries on populated paths
        addStatsToSource(
          id = SparqlTriggeredProcessor.sparqlMaterializerLabel,
          initialDownloadStats = tokensAndStatistics.materializedStats,
          source = SparqlProcessor.createSparqlSourceFromPaths(
            baseUrl = baseUrl,
            sparqlQuery = processedConfig.sparqlMaterializer,
            spQueryParamsBuilder = (p: Seq[String]) => {
              "sp.pid=" + p.head.substring(p.head.lastIndexOf('-') + 1) +
                "&sp.path=" + p.head.substring(p.head.lastIndexOf('/') + 1)
            },
            source = sensorSource,
            isNeedWrapping = false,
            label = Some(
              label
                .map(l => s"$l-${SparqlTriggeredProcessor.sparqlMaterializerLabel}")
                .getOrElse(SparqlTriggeredProcessor.sparqlMaterializerLabel)
            )
          )
        )
      }
    }
  }
}
