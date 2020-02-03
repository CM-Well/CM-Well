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

import java.nio.file.{Files, Paths}
import java.util.Calendar

import akka.actor.{ActorRef, Props}
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import cmwell.tools.data.ingester.Ingester.IngesterRuntimeConfig
import cmwell.tools.data.ingester._
import cmwell.tools.data.sparql.SparqlProcessorMain.Opts
import cmwell.tools.data.sparql.SparqlTriggeredProcessor.preProcessConfig
import cmwell.tools.data.utils.akka.{concatByteStrings, _}
import cmwell.tools.data.utils.chunkers.GroupChunker
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.tools.data.utils.ops.getVersionFromManifest
import cmwell.tools.data.utils.akka.Implicits._
import cmwell.tools.data.utils.akka.stats.{DownloaderStats, IngesterStats}
import net.jcazevedo.moultingyaml._
import org.rogach.scallop.ScallopConf

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object SparqlTriggeredProcessorMain extends App with DataToolsLogging {
  object Opts extends ScallopConf(args) {
    version(s"cm-well sparql-processor-agent ${getVersionFromManifest()} (c) 2016")

    val srcHost = opt[String]("source-host", descr = "source cm-well host name server", required = true)
    val dstHost = opt[String]("dest-host", descr = "destination cm-well host name server")
    val writeToken = opt[String]("write-token", descr = "cm-well write permission token")
    val stateFilePath = opt[String]("state", descr = "path to token state file")
    val bulk = opt[Boolean]("bulk", default = Some(false), descr = "use bulk consumer mode in download")
    val configPath = opt[String]("config", descr = "sensors config file path", required = true)
    val ingest = opt[Boolean]("ingest", descr = "ingest data to destination cm-well instance", default = Some(false))
    val appConfig = opt[String]("app-config", descr = "application.conf file custom location")
    val numConnections = opt[Int]("num-connections", descr = "number of http connections to open")
    val validationFilter =
      opt[List[String]]("validation-predicates", descr = "strings which must be present in each triple")
    val validationTrigger =
      opt[List[String]]("validation-trigger", descr = "fields in infoton which causes validation check")
    val webPort = opt[Int]("web-port", descr = "http monitor port", default = Some(8080))

    dependsOnAll(ingest, List(dstHost))
    dependsOnAll(writeToken, List(ingest, dstHost))
    verify()
  }

  // resize akka http connection pool
  Opts.numConnections.map(
    numConnections => System.setProperty("akka.http.host-connection-pool.max-connections", numConnections.toString)
  )

  // set application.conf file
  Opts.appConfig.foreach { System.setProperty("config.file", _) }

  val config = getSensorsConfig(Opts.configPath())

  val tokenFileReporter = system.actorOf(Props(new FileReporterActor(Opts.stateFilePath.toOption, Opts.webPort())))

  var lastTimeOfDataUpdate = System.currentTimeMillis()

  val processedConfig = Await.result(preProcessConfig(config, Some(tokenFileReporter)), 3.minutes)

  val processor = SparqlProcessor.createSparqlSourceFromPaths(
    baseUrl = Opts.srcHost(),
    sparqlQuery = processedConfig.sparqlMaterializer,
    spQueryParamsBuilder = SparqlTriggeredProcessor.stpSpQueryBuilder,
    source = SparqlTriggeredProcessor.createSensorSource(
      config = processedConfig,
      initialTokensAndStatistics = Right(AgentTokensAndStatistics(Map.empty[String, TokenAndStatistics],None,None)),
      tokenReporter = Some(tokenFileReporter),
      baseUrl = Opts.srcHost(),
      isBulk = Opts.bulk(),
      distinctWindowSize = 10.seconds,
      infotonGroupSize = 100
    ),
    isNeedWrapping = false,
    label = Some(
      label
        .map(l => s"$l-${SparqlTriggeredProcessor.sparqlMaterializerLabel}")
        .getOrElse(SparqlTriggeredProcessor.sparqlMaterializerLabel)
    )
  )
  .map { case (data, _) => data }
  .via(GroupChunker(GroupChunker.formatToGroupExtractor("ntriples")))
  .filter { lines =>
    val isNeedValidation = Opts.validationTrigger
      .map { trigger =>
        trigger.forall { predicate =>
          lines.exists(_ containsSlice predicate)
        }
      }
      .getOrElse(true)

//      logger.info(s"${lines(0).utf8String.takeWhile(_ != ' ')} needs validation=$isNeedValidation")

    if (isNeedValidation) {
      Opts.validationFilter
        .map { validator =>
          val isValidInfoton = validator.forall(predicate => lines.exists(_ containsSlice ByteString(predicate)))

//          if (!isValidInfoton) {
//            redLogger.error("infoton is not valid for ingest: {}", concatByteStrings(lines, endl).utf8String)
//          } else {
//            logger.info(s"${lines(0).utf8String.takeWhile(_ != ' ')} is valid for ingest")
//          }

          isValidInfoton
        }
        .getOrElse(true)
    } else {
//        logger.info(s"infoton ${lines(0).utf8String.takeWhile(_ != ' ')} does not need validation")
      true
    }
  }
  .map(concatByteStrings(_, endl))
//    .map { s => logger.info(s"lines ${s.utf8String} are valid!"); s}

  // check if need to ingest result infotons
  val processResult = if (Opts.ingest()) {

    processor.map(_ -> None).via(DownloaderStats(format = "ntriples")).map( _._1 -> None)
      .groupedWeightedWithin((25*1024), 10.seconds)(_._1.size)
      .via(Ingester.ingesterFlow(baseUrl = Opts.dstHost(),
        format = SparqlProcessor.format,
        writeToken = Opts.writeToken.toOption,
        extractContext = (_: Any) => IngesterRuntimeConfig(true)
      ))
      .map { d => d._1}
      .async
      .via(IngesterStats(isStderr = true))
      .runWith(Sink.ignore)


  } else {
    processor
      .map { infoton =>
        // scalastyle:off
        println(infoton.utf8String)
        // scalastyle:on
        infoton
      } // print to stdout
      .map(_ -> None)
      .via(DownloaderStats(format = "ntriples"))
      .runWith(Sink.ignore)
  }

  processResult.onComplete {
    case Success(_) => logger.info("SparqlTriggeredProcessor finished with success")
    case Failure(e) => logger.error("SparqlTriggeredProcessor failed with an exception: ", e)
  }

  def getSensorsConfig(path: String): Config = {
    // parse sensor configuration
    object SensorYamlProtocol extends DefaultYamlProtocol {
      implicit object DurationYamlFormat extends YamlFormat[FiniteDuration] {
        override def write(obj: FiniteDuration): YamlValue = YamlObject(
          YamlString("updateFreq") -> YamlString(obj.toString)
        )

        override def read(yaml: YamlValue): FiniteDuration = {
          val d = Duration(yaml.asInstanceOf[YamlString].value)
          FiniteDuration(d.length, d.unit)
        }
      }

      implicit val sensorFormat = yamlFormat6(Sensor)
      implicit val sequenceFormat = seqFormat[Sensor](sensorFormat)
      implicit val configFormat = yamlFormat7(Config)
    }

    import SensorYamlProtocol._
    import net.jcazevedo.moultingyaml._

    val yamlConfig = new String(Files.readAllBytes(Paths.get(path))).parseYaml

    yamlConfig.convertTo[Config]
  }
}
