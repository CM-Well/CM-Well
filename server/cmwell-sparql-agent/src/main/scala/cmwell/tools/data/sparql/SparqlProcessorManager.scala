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

import java.nio.file.Paths

import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import cmwell.ctrl.checkers.StpChecker.{RequestStats, ResponseStats, Row, Table}
import cmwell.tools.data.downloader.consumer.Downloader.Token
import cmwell.tools.data.ingester._
import cmwell.tools.data.sparql.InfotonReporter.{RequestDownloadStats, ResponseDownloadStats}
import cmwell.tools.data.sparql.SparqlProcessorManager._
import cmwell.tools.data.utils.ArgsManipulations
import cmwell.tools.data.utils.ArgsManipulations.HttpAddress
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.chunkers.GroupChunker
import cmwell.tools.data.utils.chunkers.GroupChunker._
import cmwell.util.string.Hash
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import k.grid.GridReceives
import net.jcazevedo.moultingyaml._
import org.apache.commons.lang3.time.DurationFormatUtils

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success}

object SparqlProcessorManager {
  val name = "sparql-triggered-processor-manager"

  val formatter = java.text.NumberFormat.getNumberInstance

  case class Job(config: Config,
                 path: Path,
                 killSwitch: KillSwitch,
                 reporter: ActorRef) {

    override def toString: String = {
      val sensors = config.sensors.map(_.name).mkString(", ")
      s"[job: ${path.utf8String}, name:${config.name}, sensors: $sensors]"
    }
  }

  type Path = ByteString
  type PathAndConfig = (Path, Config)
  type PathAndJob = (Path, Job)
  type Configs = Map[Path, Config]
  type Jobs = Map[Path, Job]

  case object CheckConfig
  case class AnalyzeReceivedConfig(received: Map[Path, Config])
  case class StartJob(registered: Boolean, path: Path, config: Config)
  case class StopJob(registered: Boolean, path: Path, job: Job)
  case class JobsToDisable(jobs: Jobs)
  case class NonActiveJobsToRemove(configs: Configs)
  case class AddToNonActive(configs: Configs)
}


class SparqlProcessorManager extends Actor with LazyLogging {
  /** properties and settings*/
  val config = ConfigFactory.load()
  val hostConfigFile         = config.getString("cmwell.agents.sparql-triggered-processor.host-config-file")
  val hostUpdatesSource      = config.getString("cmwell.agents.sparql-triggered-processor.host-updates-source")
  val hostWriteOutput        = config.getString("cmwell.agents.sparql-triggered-processor.host-write-output")
  val materializedViewFormat = config.getString("cmwell.agents.sparql-triggered-processor.format")
  val pathAgentConfigs       = config.getString("cmwell.agents.sparql-triggered-processor.path-agent-configs")
  val writeToken             = config.getString("cmwell.agents.sparql-triggered-processor.write-token")
  val initDelay = {
    val Duration(length, timeUnit) = Duration(config.getString("cmwell.agents.sparql-triggered-processor.init-delay"))
    FiniteDuration(length, timeUnit)
  }
  val interval = {
    val Duration(length, timeUnit) = Duration(config.getString("cmwell.agents.sparql-triggered-processor.config-polling-interval"))
    FiniteDuration(length, timeUnit)
  }

  var configMonitor: Cancellable = _
  var activeJobs: Jobs = Map()
  var nonActiveConfigs: Configs = Map()

  val HttpAddress(protocol, host, port, _) = ArgsManipulations.extractBaseUrl(hostConfigFile)

  implicit val system = context.system
  implicit val mat = ActorMaterializer()
  implicit val ec = context.dispatcher

  val httpPoolConfig = {
    val HttpAddress(protocol, host, port, _) = ArgsManipulations.extractBaseUrl(hostConfigFile)
    Http().cachedHostConnectionPool[ByteString](host, port)
  }


  override def preStart(): Unit = {
    configMonitor = context.system.scheduler.schedule(initDelay, interval, self, CheckConfig)
    logger.info("starting sparql-processor manager instance on this machine")
  }

  def yamlToConfig(yaml: String) = {
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
      implicit val configFormat = yamlFormat4(Config)
    }

    import SensorYamlProtocol._
    import net.jcazevedo.moultingyaml._

    val yamlConfig = yaml.parseYaml

    yamlConfig.convertTo[Config]
  }

  def receive = {
    import akka.pattern._

    GridReceives.monitoring(sender).orElse {
      case CheckConfig =>
        getYamlConfigs().map(AnalyzeReceivedConfig.apply) pipeTo self

      case AnalyzeReceivedConfig(received) =>
        handleSensors {
          received.map { case (path, config) =>
            val configName = Paths.get(path.utf8String).getFileName
            val sensors = config.sensors.map(sensor => sensor.copy(name = s"$configName-${sensor.name}"))
            path -> config.copy(sensors = sensors)
          }
        }
//        handleSensors(received)

      case StartJob(registered, path, config) if registered =>
        val job = startJob(path, config)
        activeJobs += (path -> job)
        nonActiveConfigs -= path

      case JobsToDisable(jobs) =>
        jobs.foreach {
          case (path, job) =>
            // stop jobs and store it in non-active job list
            job.killSwitch.abort(new Exception("job interrupted"))
            job.reporter! PoisonPill
            activeJobs -= path

            nonActiveConfigs += (path -> job.config)
        }

      case AddToNonActive(configs) =>
        nonActiveConfigs ++= configs

      case NonActiveJobsToRemove(configs) =>
        nonActiveConfigs --= configs.keySet

      case StopJob(registered, path, job) if registered =>
        job.killSwitch.abort(new Exception("job interrupted"))

        activeJobs -= path
        job.killSwitch.shutdown()
        job.reporter ! PoisonPill

      case RequestStats =>
        stringifyActiveJobs().map(ResponseStats.apply) pipeTo sender()
    }
  }

  /**
    * Analyse configuration read from infotons
    * @param received configuration read from infotons
    */
  def handleSensors(received: Configs) = {
    // get stored 'active' status of jobs
    val storedJobPathsWithStatus = getActiveStatusOfStoredJobs()

    // disable previous non-active jobs (on load time of the actor)
    storedJobPathsWithStatus.map { storedJobs =>
      val nonActiveStoredJobs = storedJobs.filter { case (_, isActive) => !isActive }
      val pathsToDisable = nonActiveStoredJobs.keySet intersect received.keySet.diff (activeJobs.keySet ++ nonActiveConfigs.keySet)
      received.filterKeys(pathsToDisable contains _)
    }.map (AddToNonActive.apply) pipeTo self

    // disable jobs which have new active=false status
    storedJobPathsWithStatus
      .map {_.filter { case (_, isActive) => !isActive } }
      .map (nonActiveStoredJobs => activeJobs filterKeys nonActiveStoredJobs.keySet )
      .map (JobsToDisable.apply) pipeTo self

    // start jobs which have a new 'active' status
    storedJobPathsWithStatus
      .map {_.filter { case (_, isActive) => isActive } }
      .map (newActiveStoredJobs => nonActiveConfigs filterKeys newActiveStoredJobs.keySet)
      .map (registerToStart)

    // stop and remove jobs which no longer exist
    val jobsToStopAndRemove = activeJobs.filterKeys(activeJobs.keySet diff received.keySet)
    registerJobsToStop(jobsToStopAndRemove)

    // remove non active jobs which are no longer exist
    self ! NonActiveJobsToRemove(nonActiveConfigs.filterKeys(nonActiveConfigs.keySet diff received.keySet))

    // find new configs (did not appear before) and register them to start
    storedJobPathsWithStatus.map { storedJobs =>
      val nonActiveStoredJobs = storedJobs.filter { case (_, isActive) => !isActive }
      val pathsToStart = received.keySet diff (activeJobs.keySet ++ nonActiveConfigs.keySet ++ nonActiveStoredJobs.keySet)
      received.filterKeys(pathsToStart contains _)
    }.map (registerToStart)
  }

  /**
    * Generates data for tables in cm-well monitor page
    */
  def stringifyActiveJobs(): Future[Iterable[Table]] = {
    implicit val timeout = Timeout(1.minute)

    def generateNonActiveTables() = nonActiveConfigs.map { case (path, config) =>
      val sensorNames = config.sensors.map(_.name)
      val title = Seq(s"""<span style="color:red"> **Non-Active** </span> ${path.utf8String}""")
      val header = Seq("sensor", "point-in-time")

      val req = HttpRequest(uri = s"http://$hostConfigFile${path.utf8String}/tokens?op=stream&recursive&format=ntriples&fields=token")

      // get stored tokens
      val storedTokensFuture: Future[Map[String, Token]] = Source.single(req -> blank)
        .via(httpPoolConfig)
        .mapAsync(1) {
          case (Success(HttpResponse(s, _, e, _)), _) if s.isSuccess() =>
            e.withoutSizeLimit().dataBytes.runFold(blank)(_ ++ _)
          case (Success(HttpResponse(s, _, e, _)), _) =>
            logger.error(s"cannot get stored tokens for non-active jobs: status=$s, entity=$e")
            e.discardBytes()
            Future.successful(blank)
          case (Failure(err), _) =>
            logger.error(s"cannot get stored tokens for non-active jobs: err=$err")
            Future.successful(blank)
        }.runWith(Sink.head)
        .map { rawData =>
          rawData.utf8String.split("\n").map(_.split(" "))
            .collect { case Array(s, p, o, _) =>
              val token = if (o.startsWith("\"")) o.init.tail else o
              val sensorName = Paths.get(s).getFileName.toString.init
              sensorName -> token
            }.toMap
        }

      storedTokensFuture.map { storedTokens =>
        val pathsWithoutSavedToken = sensorNames.toSet diff storedTokens.keySet
        val allSensorsWithTokens = storedTokens ++ pathsWithoutSavedToken.map(_ -> "")

        val body = allSensorsWithTokens.map { case (sensorName, token) =>
          val decodedToken = if (token.nonEmpty) {
            val from = cmwell.tools.data.utils.text.Tokens.getFromIndexTime(token)
            new org.joda.time.DateTime(from).toString
          }
          else ""

          val row: Row = Seq(sensorName, decodedToken)
          row
        }

        Table(title = title, header = header, body = body)
      }
    }

    def generateActiveTables() = activeJobs.map { case (path, job) =>
      val title = Seq(s"""<span style="color:green"> **Active** </span> ${path.utf8String}""")
      val header = Seq("sensor", "point-in-time", "received-infotons", "infoton-rate", "last-update")

      val statsFuture = (job.reporter ? RequestDownloadStats).mapTo[ResponseDownloadStats]
        .map { case ResponseDownloadStats(stats) => stats }

      val storedTokensFuture = (job.reporter ? RequestPreviousTokens).mapTo[ResponseWithPreviousTokens]
        .map { case ResponseWithPreviousTokens(storedTokens) => storedTokens }

      for {
        stats <- statsFuture
        storedTokens <- storedTokensFuture
      } yield {
        val sensorNames = job.config.sensors.map(_.name)
        val pathsWithoutSavedToken = sensorNames.toSet diff storedTokens.keySet
        val allSensorsWithTokens = storedTokens ++ pathsWithoutSavedToken.map(_ -> "")

        val body = allSensorsWithTokens.map { case (sensorName, token) =>
          val decodedToken = if (token.nonEmpty) {
            val from = cmwell.tools.data.utils.text.Tokens.getFromIndexTime(token)
            new org.joda.time.DateTime(from).toString
          }
          else ""

          val sensorStats = stats.get(sensorName).map { s =>
            val statsTime = new org.joda.time.DateTime(s.statsTime).toString
            Seq(s.receivedInfotons.toString, s"${formatter.format(s.infotonRate)}/sec", statsTime)
          }.getOrElse(Seq.empty[String])

          val row: Row = Seq(sensorName, decodedToken) ++ sensorStats
          row
        }

        val configName = Paths.get(path.utf8String).getFileName
        val sparqlMaterializerStats = stats.get(s"$configName-${SparqlTriggeredProcessor.sparqlMaterializerLabel}").map { s =>
          val totalRunTime = DurationFormatUtils.formatDurationWords(s.runningTime, true, true)
          s"""Materialized <span style="color:green"> **${s.receivedInfotons}** </span> infotons [$totalRunTime]""".stripMargin
        }.getOrElse("")

        Table(title = title :+ sparqlMaterializerStats, header = header, body = body)
      }
    }

    // generate data for both active and non-active jobs
    Future.sequence {
      generateNonActiveTables() ++ generateActiveTables()
    }
  }

  /**
    * Execute a new job from a given config
    * @param path the path of the config
    * @param config
    * @return
    */
  def startJob(path: Path, config: Config): Job = {
    val configName = Paths.get(path.utf8String).getFileName

    val tokenReporter = context.actorOf(
      props = Props(new InfotonReporter(baseUrl = hostConfigFile, path = path.utf8String)),
      name = s"$configName-${Hash.crc32(config.toString)}"
    )

    val agent = SparqlTriggeredProcessor.listen(config, hostUpdatesSource, true, tokenReporter, Some(configName.toString))
      .map { case (data, _) => data }
      .via(GroupChunker(formatToGroupExtractor(materializedViewFormat)))
      .map(concatByteStrings(_, endl))

    val killSwitch = Ingester.ingest(baseUrl = hostWriteOutput,
      format = materializedViewFormat,
      source = agent,
      label = Some(s"ingester-${configName.toString}")
    )
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.left)
      .run()

    logger.info(s"created job: ${config.sensors.map(_.name).mkString(", ")}")

    Job (
      path = path,
      config = config,
      killSwitch = killSwitch,
      reporter = tokenReporter
    )
  }

  def getYamlConfigs() = {
    logger.info("Checking the current status of the config infotons")

    /**
      * Gets config string read from json infoton
      * @param json raw json bytes read from infoton
      * @return config string extracted from infoton json bytes
      */
    def extractConfigStringFromInfotonData(json: ByteString): String = {
      import spray.json._
      JsonParser(ParserInput(json.toArray))
        .asJsObject.fields("content")
        .asJsObject.fields("data") match {
          case JsString(config) => config
          case _ => ""
        }
    }

    /**
      * Gets YAML config from a given configuration path
      * @return path and its YAML configuration
      */
    def getInfotonDataFromPath() = {
      akka.stream.scaladsl.Flow[Path]
        .map(p => HttpRequest(uri = s"http://$hostConfigFile${p.utf8String}/config?with-data&format=json") -> p)
        .via(httpPoolConfig)
        .mapAsyncUnordered(1){
          case (Success(HttpResponse(status, _, entity, _)), path) if status.isSuccess() =>
            val infotonData = entity.dataBytes.runFold(blank)(_ ++ _)
            infotonData.map(d => Some(path -> d))
          case _ =>
            logger.error("cannot get config for sparql-triggered-processor")
            Future.successful(None)
      }
    }

    getPathsOfConfigInfotons()
      .via(getInfotonDataFromPath())
      .map { case Some((path, data)) => path -> extractConfigStringFromInfotonData(data) }
      .collect { case (path, configString) if configString.nonEmpty =>
        path -> yamlToConfig(configString)
      }
      .runFold(Map.empty[Path, Config])(_ + _)
  }

  /**
    * Gets all paths of configuration infotons
    */
  def getPathsOfConfigInfotons(): Source[Path, _] = {
    val request = HttpRequest(uri = s"http://$hostConfigFile$pathAgentConfigs/?op=search&with-data&format=text")
    Source.single(request -> blank)
      .via(httpPoolConfig)
      .flatMapConcat {
        case (Success(HttpResponse(status, _, entity, _)), _) if status.isSuccess() =>
          entity.dataBytes
            .via(lineSeparatorFrame)
            .filter(_.nonEmpty)
      }
  }

  def writeActiveStatus(infotonPath: Path, active: Boolean): Future[Boolean] = {
    val triple =
      s"""<http://$hostConfigFile${infotonPath.utf8String}> <http://$hostConfigFile/meta/nn#active> "$active"^^<http://www.w3.org/2001/XMLSchema#boolean> ."""
    val req = HttpRequest(method = POST, uri = "/_in?format=ntriples&replace-mode", entity = triple)
      .addHeader(RawHeader("X-CM-WELL-TOKEN", writeToken))

    Source.single(Seq(blank) -> None)
      .via(Retry.retryHttp(2.seconds, 1, hostConfigFile)(_ => req))
      .mapAsync(1) {
        case (Success(HttpResponse(s, _, e, _)), _, _) if s.isSuccess() =>
          e.discardBytes().future().map(_ => true)
        case (Success(HttpResponse(s, _, e, _)), _, _) =>
          logger.error(s"cannot update 'active' triple in ${infotonPath.utf8String}")
          e.discardBytes().future().map(_ => false)
        case x =>
          logger.error(s"unexpected data: $x")
          Future.successful(false)
      }
      .runWith(Sink.head)
  }

  def registerJobsToStop(jobs: Jobs): Unit = jobs.foreach { case (path, job) =>
    logger.info(s"stopping sensors: ${job.config.sensors.map(_.name).mkString(", ")}")

    writeActiveStatus(path, false).map(StopJob(_, path, job)) pipeTo self
  }

  def registerToStart(configs: Configs): Unit = configs.map { case (path, config) =>
    logger.info(s"register job: ${config.sensors.map(_.name).mkString(", ")}")

    val jobFuture = writeActiveStatus(path, true)
      .map(registered => StartJob(registered = registered, config = config, path = path))

    jobFuture pipeTo self
  }

  def getActiveStatusOfStoredJobs(): Future[Map[Path, Boolean]] = {

    def getActiveStatusOfStoredConfig(): Flow[Path, (Path, ByteString), _] = {
      akka.stream.scaladsl.Flow[Path]
        .map(p => HttpRequest(uri = s"http://$hostConfigFile${p.utf8String}?&with-data&format=json&fields=active") -> p)
        .via(httpPoolConfig)
        .mapAsyncUnordered(1){
          case (Success(HttpResponse(status, _, entity, _)), path) if status.isSuccess() =>
            val infotonData = entity.dataBytes.runFold(blank)(_ ++ _)
            infotonData.map(d => Some(path -> d))
          case (_, path) =>
            logger.error(s"cannot get config for sparql-triggered-processor for path=${path.utf8String}")
            Future.successful(None)
        }
        .collect { case Some((path, infotonData)) => path -> infotonData}
    }

    def extractActiveStringFromInfoton(json: ByteString) = {
      import spray.json._

      val parsed = JsonParser(ParserInput(json.toArray)).asJsObject

      if (!parsed.fields.contains("fields")) ""
      else {
        val result = JsonParser(ParserInput(json.toArray))
          .asJsObject.fields("fields")
          .asJsObject.fields("active") match {
          case JsArray(data) => data.map {
            _ match {
              //        if (!x.asJsObject.fields.contains("active")) ""
              //        else x.asJsObject.fields("active").toString()

              case JsTrue => "true"
              case JsFalse => "false"
              case _ => ""
            }
          }

          case x => Seq.empty[String]
        }

        result.mkString("\n")
      }
    }

    getPathsOfConfigInfotons()
      .via(getActiveStatusOfStoredConfig())
      .map{ case (path, infotonData) => path -> extractActiveStringFromInfoton(infotonData) }
      .runFold(Map.empty[Path, String])(_ + _)
      .map(_.filter{case (path, activeStatus) => path.nonEmpty && activeStatus.nonEmpty})
      .map(_.mapValues(_.toBoolean))
  }

  override def postStop(): Unit = {
    logger.info(s"${this.getClass.getSimpleName} is about to die, stopping all active jobs")
    registerJobsToStop(activeJobs)

    configMonitor.cancel()
  }
}

