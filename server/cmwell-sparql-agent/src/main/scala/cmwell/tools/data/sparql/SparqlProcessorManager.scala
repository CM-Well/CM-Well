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

import java.nio.file.Paths
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneId}

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import cmwell.ctrl.checkers.StpChecker.{RequestStats, ResponseStats, Row, Table}
import cmwell.driver.Dao
import cmwell.tools.data.ingester._
import cmwell.tools.data.sparql.InfotonReporter.{RequestDownloadStats, RequestIngestStats, ResponseDownloadStats, ResponseIngestStats}
import cmwell.tools.data.sparql.SparqlProcessorManager._
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.akka.stats.{DownloaderStats, IngesterStats}
import cmwell.tools.data.utils.text.Tokens
import cmwell.util.concurrent._
import cmwell.util.http.SimpleResponse
import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
import cmwell.util.string.Hash
import cmwell.zstore.ZStore
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import k.grid.GridReceives
import net.jcazevedo.moultingyaml._
import org.apache.commons.lang3.time.DurationFormatUtils
import akka.stream.scaladsl.Flow
import cmwell.tools.data.ingester.Ingester.IngesterRuntimeConfig
import cmwell.tools.data.sparql.SparqlProcessor.SparqlRuntimeConfig
import cmwell.tools.data.utils.akka.Retry.State

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success, Try}

case class Job(name: String, config: Config) {
  val jobString = {
    val sensors = config.sensors.map(_.name).mkString(", ")
    s"[job: $name, config name:${config.name}, sensors: $sensors]"
  }
  override def toString: String = jobString
}

case class JobRead(job: Job, active: Boolean)
case class StartJob(job: JobRead)
case class JobHasFailed(job: Job, ex: Throwable)
case class JobHasFinished(job: Job)
case class PauseJob(job: Job)
case class StopAndRemoveJob(job: Job)

case object CheckConfig
case class AnalyzeReceivedJobs(jobsRead: Set[JobRead])

sealed trait JobStatus {
  val statusString: String
  val job: Job
  val canBeRestarted: Boolean = false
}

sealed trait JobActive extends JobStatus {
  val reporter: ActorRef
}

case class JobRunning(job: Job, killSwitch: KillSwitch, reporter: ActorRef) extends JobActive {
  override val statusString = "Running"
}
case class JobPausing(job: Job, killSwitch: KillSwitch, reporter: ActorRef) extends JobActive {
  override val statusString = "Pausing"
}
case class JobStopping(job: Job, killSwitch: KillSwitch, reporter: ActorRef) extends JobActive {
  override val statusString: String = "Stopping"
}

case class JobFailed(job: Job, ex: Throwable) extends JobStatus {
  override val statusString = "Failed"
  override val canBeRestarted = true
}
case class JobPaused(job: Job) extends JobStatus {
  override val canBeRestarted = true
  override val statusString = "Paused"
}

object SparqlProcessorManager {
  val name = "sparql-triggered-processor-manager"
  private val formatter = java.text.NumberFormat.getNumberInstance

  val client = cmwell.util.http.SimpleHttpClient
}

class SparqlProcessorManager(settings: SparqlProcessorManagerSettings) extends Actor with LazyLogging {

  type Jobs = Map[String, JobStatus]

  var currentJobs: Jobs = Map.empty
  var configMonitor: Cancellable = _

  //todo: should we use injected ec??? implicit val ec = context.dispatcher
  implicit val system: ActorSystem = context.system
  implicit val mat = ActorMaterializer()

  // HTTP Connection Pool, specific to particular HTTP endpoint
  type ConnectionPool = Flow[(HttpRequest,State[(Option[StpMetadata],Long)]),
    (Try[HttpResponse], State[(Option[StpMetadata],Long)]), Http.HostConnectionPool]

  // A (pool) of reusable HTTP connection pools for a number of HTTP endpoints
  var connectionPools : Map[String,ConnectionPool] = Map.empty

  if (mat.isInstanceOf[ActorMaterializer]) {
    require(mat.asInstanceOf[ActorMaterializer].system eq context.system,
      "ActorSystem of materializer MUST be the same as the one used to create current actor")
  }

  lazy val stpDao = Dao(settings.irwServiceDaoClusterName, settings.irwServiceDaoKeySpace2, settings.irwServiceDaoHostName, 9042, initCommands = None)
  lazy val zStore : ZStore = ZStore.apply(stpDao)

  def extractSparqlContext(context: Option[(Option[StpMetadata], Long)]) = context match {
    case Some((Some(metadata), _)) => {
      SparqlRuntimeConfig(
        hostUpdatesSource = metadata.agentConfig.hostUpdatesSource.get,
        useQuadsInSp = metadata.agentConfig.useQuadsInSp.getOrElse(settings.useQuadsInSp),
        label = metadata.agentConfig.name,
        sparqlMaterializer =  metadata.agentConfig.sparqlMaterializer
      )
    }
    case Some((None, _)) => logger.error("metadata is None!"); ???
    case None => logger.error("Context(metadata) is None!"); ???
  }

  val (stpAgentSink, postIngestSource) =
    MergeHub.source[(ByteString,Option[StpMetadata])](perProducerBufferSize = 256)
      .groupBy(2, {
        case (_, Some(stpMetadata)) =>
          stpMetadata.agentConfig.force.getOrElse(false)
        case (_, None) => logger.error("stpMetadata is None"); ???
      } )
    .groupedWeightedWithin((25*1024), 10.seconds)(_._1.size)
    .via(
      Ingester.ingesterFlow(baseUrl = settings.hostWriteOutput,
        format = settings.materializedViewFormat,
        writeToken = Option(settings.writeToken),
        extractContext = (context: Option[StpMetadata]) => context match {
          case Some(stpMetadata) => IngesterRuntimeConfig(stpMetadata.agentConfig.force.getOrElse(false),
            stpMetadata.agentConfig.name)
          case None => logger.error("Context(stpMetadata) is None!"); ???
        },
        connectionPool = Some(Retry.createNewHostConnectionPool[StpMetadata](settings.hostWriteOutput))
      )
    )
    .mergeSubstreams
    .toMat(BroadcastHub.sink(bufferSize = 256))(Keep.both).run


  val pubSubChannel = Flow.fromSinkAndSource(stpAgentSink, postIngestSource)
    .joinMat(KillSwitches.singleBidi[(Ingester.IngestEvent, Option[StpMetadata]),(ByteString, Option[StpMetadata]) ])(Keep.right)

  override def preStart(): Unit = {
    logger.info("starting sparql-processor manager instance on this machine")
    configMonitor = context.system.scheduler.schedule(settings.initDelay, settings.interval, self, CheckConfig)
  }

  override def receive: Receive = {
    import akka.pattern._
    GridReceives.monitoring(sender).orElse {
      case RequestStats(isAdmin: Boolean)        => stringifyActiveJobs(currentJobs, isAdmin).map(ResponseStats.apply).pipeTo(sender())
      case CheckConfig                           => getJobConfigsFromTheUser.map(AnalyzeReceivedJobs.apply).pipeTo(self)
      case AnalyzeReceivedJobs(jobsReceived)     => handleReceivedJobs(jobsReceived, currentJobs)
      case Status.Failure(e)                     => logger.warn("Received Status failure ", e)
      case StartJob(job)                         => handleStartJob(job)
      case JobHasFinished(job: Job)              => handleJobHasFinished(job)
      case JobHasFailed(job: Job, ex: Throwable) => handleJobHasFailed(job, ex)
      case PauseJob(job: Job)                    => handlePauseJob(job)
      case StopAndRemoveJob(job: Job)            => handleStopAndRemoveJob(job)

      case other => logger.error(s"received unexpected message: $other")
    }
  }

  /**
    * This method MUST be run from the actor's thread (it changes the actor state)!
    * @param job
    */
  def handleJobHasFinished(job: Job): Unit = {
    currentJobs
      .get(job.name)
      .fold {
        logger.error(
          s"Got finished signal for job $job that doesn't exist in the job map. Not reasonable! Current job in map are: ${currentJobs.keys
            .mkString(",")}"
        )
      } {
        case JobPausing(runningJob, _, _) =>
          logger.info(s"Job $runningJob has finished. Saving the job state.")
          currentJobs = currentJobs + (job.name -> JobPaused(runningJob))
        case JobStopping(runningJob, _, _) =>
          logger.info(s"Job $runningJob has finished. Removing the job from the job list.")
          currentJobs = currentJobs - runningJob.name
        case other =>
          logger.error(
            s"Got finished signal for jog $job but the actual state in current jobs is $other. Not reasonable!"
          )
          currentJobs = currentJobs - job.name
      }
  }

  /**
    * This method MUST be run from the actor's thread (it changes the actor state)!
    * @param job
    * @param ex
    */
  def handleJobHasFailed(job: Job, ex: Throwable): Unit = {
    currentJobs
      .get(job.name)
      .fold {
        logger.error(
          s"Got failed signal for job $job that doesn't exist in the job map. Not reasonable! Current job in map are: ${currentJobs.keys
            .mkString(",")}"
        )
      } {
        case _: JobRunning | _: JobPausing | _: JobStopping =>
          logger.info(s"Job $job has failed. Saving the job failure in current jobs.")
          currentJobs = currentJobs + (job.name -> JobFailed(job, ex))
        case other =>
          logger.error(
            s"Got failed signal for jog $job but the actual state in current jobs is $other. Not reasonable!"
          )
          currentJobs = currentJobs - job.name
      }
  }

  /**
    * This method MUST be run from the actor's thread (it changes the actor state)!
    * @param job
    */
  def handlePauseJob(job: Job): Unit = {
    currentJobs
      .get(job.name)
      .fold {
        logger.error(
          s"Got pause request for job $job that doesn't exist in the job map. Not reasonable! Current job in map are: ${currentJobs.keys
            .mkString(",")}"
        )
      } {
        case JobRunning(runningJob, killSwitch, reporter) =>
          logger.info(
            s"Pausing job $runningJob. The job will actually pause only after it will finish all its current operations"
          )
          currentJobs = currentJobs + (job.name -> JobPausing(job, killSwitch, reporter))
          killSwitch.shutdown()
        case other =>
          logger.error(
            s"Got pause request for jog $job but the actual state in current jobs is $other. Not reasonable!"
          )
      }
  }

  /**
    * This method MUST be run from the actor's thread (it changes the actor state)!
    * @param job
    */
  def handleStopAndRemoveJob(job: Job): Unit = {
    currentJobs
      .get(job.name)
      .fold {
        logger.error(
          s"Got stop and remove request for job $job that doesn't exist in the job map. Not reasonable! Current job in map are: ${currentJobs.keys
            .mkString(",")}"
        )
      } {
        case JobRunning(runningJob, killSwitch, reporter) =>
          logger.info(
            s"Stopping job $runningJob. The job will actually stopped only after it will finish all its current operations"
          )
          currentJobs = currentJobs + (job.name -> JobStopping(job, killSwitch, reporter))
          killSwitch.shutdown()
        case JobFailed(failedJob, _) =>
          logger.info(s"Stopping job $failedJob. The job has already failed. Removing it from the job list.")
          currentJobs = currentJobs - job.name
        case JobPaused(pausedJob) =>
          logger.info(s"Stopping job $pausedJob. The job is currently paused. Removing it from the job list.")
          currentJobs = currentJobs - job.name
        case other =>
          logger.error(
            s"Got stop and remove request for jog $job but the actual state in current jobs is $other. Not reasonable!"
          )
      }
  }

  def shouldStartJob(currentJobs: Map[String, JobStatus])(jobRead: JobRead): Boolean = {
    jobRead.active && currentJobs.get(jobRead.job.name).fold(true)(_.canBeRestarted)
  }

  def handleReceivedJobs(jobsRead: Set[JobRead], currentJobs: Map[String, JobStatus]): Unit = {
    //jobs to start
    val jobsToStart = jobsRead.filter(shouldStartJob(currentJobs))
    jobsToStart.foreach { jobRead =>
      logger.info(s"Got start request for job ${jobRead.job} from the user")
      self ! StartJob(jobRead)
    }
    //jobs to pause or to totally remove
    currentJobs.foreach {
      case (currentJobName, currentJob @ (_: JobRunning | _: JobFailed | _: JobPaused))
        if !jobsRead.exists(_.job.name == currentJobName) =>
        logger.info(s"Got stop and remove request for job ${currentJob.job} from the user")
        self ! StopAndRemoveJob(currentJob.job)
      case (currentJobName, jobRunning: JobRunning) if !jobsRead.find(_.job.name == currentJobName).get.active =>
        logger.info(s"Got pause request for job ${jobRunning.job} from the user")
        self ! PauseJob(jobRunning.job)
      case _ => //nothing to do on other cases
    }
  }

  /**
    * Generates data for tables in cm-well monitor page
    */
  def stringifyActiveJobs(jobs: Jobs, isAdmin: Boolean = false): Future[Iterable[Table]] = {
    implicit val timeout = Timeout(1.minute)

    def generateNonActiveTables(jobs: Jobs) = jobs.collect {
      case (path, jobStatus @ (_: JobPaused | _: JobFailed)) =>
        val (colour, status) = jobStatus match {
          case jobFailed @ (_: JobFailed) =>
            ("red", "Exception : " + jobFailed.ex.getMessage)
          case _: JobPaused =>
            ("green", "No exceptions reported")
          case x @ (JobPausing(_, _, _) | JobRunning(_, _, _) | JobStopping(_, _, _)) => logger.error(s"Unexpected Job Status: $x"); ???
        }

        val sensorNames = jobStatus.job.config.sensors.map(_.name)
        val title = Seq(
          s"""<span style="color:${colour}"> **Non-Active - ${jobStatus.statusString} ** </span> ${path} <br/><span style="color:${colour}">${status}</span>"""
        )

        val controls = isAdmin match {
          case true => s" (<a href='/zz/stp-agent-$path?op=purge'>Reset Tokens</a>)"
          case false => ""
        }

        val header = Seq("Sensor", "Token Time" + controls)

        StpUtil.readPreviousTokens(settings.hostConfigFile, settings.pathAgentConfigs + "/" + path, zStore).map {
          result =>
            result.sensors match {
              case storedTokens =>
                val pathsWithoutSavedToken = sensorNames.toSet.diff(storedTokens.keySet)
                val allSensorsWithTokens = storedTokens ++ pathsWithoutSavedToken.map(_ -> ("", None))

                val body: Iterable[Row] = allSensorsWithTokens.map {
                  case (sensorName, (token, _)) =>
                    val decodedToken = if (token.nonEmpty) {
                      val from = cmwell.tools.data.utils.text.Tokens.getFromIndexTime(token)
                      LocalDateTime.ofInstant(Instant.ofEpochMilli(from), ZoneId.systemDefault()).toString
                    } else ""

                    Seq(sensorName, decodedToken)
                }

                val controls = s"<a href='/_stp/$path?enabled=true'>Resume</a>"

                Table(title = title :+ controls, header = header, body = body)
            }
        }.recover {
          case _ => Table(title = title, header = header, body = Seq(Seq("")))
        }
    }

    def generateActiveTables(jobs: Jobs) = jobs.collect {
      case (path, jobStatus @ (_: JobActive)) =>
        val jobConfig = jobStatus.job.config


        val hostUpdatesSource = jobConfig.hostUpdatesSource.getOrElse(settings.hostUpdatesSource)

        val title = Seq(
          s"""<span style="color:green"> **${jobStatus.statusString}** </span> Agent: ${path} Source: ${hostUpdatesSource}"""
        )
        val header = Seq("Sensor", "Token Time", "Received Infotons", "Remaining Infotons", "Infoton Rate", "Statistics Updated")
        val statsFuture = (jobStatus.reporter ? RequestDownloadStats).mapTo[ResponseDownloadStats]
        val storedTokensFuture = (jobStatus.reporter ? RequestPreviousTokens).mapTo[ResponseWithPreviousTokens]

        val stats2Future = (jobStatus.reporter ? RequestIngestStats).mapTo[ResponseIngestStats]

        for {
          statsRD <- statsFuture
          stats = statsRD.stats
          statsIngestRD <- stats2Future
          statsIngest = statsIngestRD.stats
          storedTokensRWPT <- storedTokensFuture
        } yield {
          storedTokensRWPT.tokens match {
            case Right(storedTokensAndStats) => {

              val sensorNames = jobConfig.sensors.map(_.name)
              val pathsWithoutSavedToken = sensorNames.toSet.diff(storedTokensAndStats.sensors.keySet)
              val allSensorsWithTokens = storedTokensAndStats.sensors ++ pathsWithoutSavedToken.map(_ -> ("", None))

              val body: Iterable[Row] = allSensorsWithTokens.map {
                case (sensorName, (token, _)) =>
                  val decodedToken = if (token.nonEmpty) {
                    Tokens.getFromIndexTime(token) match {
                      case 0 => ""
                      case tokenTime =>
                        (LocalDateTime.ofInstant(Instant.ofEpochMilli(tokenTime), ZoneId.systemDefault()).toString)
                    }
                  } else ""

                  val sensorStats = stats
                    .get(sensorName)
                    .map { s =>
                      val statsTime = s.statsTime match {
                        case 0 => s"""<span style="color:red">Not Yet Updated</span>"""
                        case _ =>

                          val updateTime =  LocalDateTime.ofInstant(Instant.ofEpochMilli(s.statsTime), ZoneId.systemDefault())
                          val currentTime = LocalDateTime.now(ZoneId.systemDefault())

                          if (updateTime.until(currentTime, ChronoUnit.MILLIS) > settings.sensorAlertDelay.toMillis && s.horizon == false) {
                            s"""<span style="color:red">${updateTime.toString}</span>"""
                          }
                          else {
                            updateTime.toString
                          }
                      }

                      val infotonRate = s.horizon match {
                        case true => s"""<span style="color:green">Horizon</span>"""
                        case false => s"${formatter.format(s.infotonRate)}/sec"
                      }

                      val remainingInfotons = s.remaining match {
                        case None => s"No Data"
                        case Some(remain) => remain.toString
                      }

                      Seq(s.receivedInfotons.toString, remainingInfotons, infotonRate, statsTime)

                    }
                    .getOrElse(Seq.empty[String])

                  Seq(sensorName, decodedToken) ++ sensorStats
              }
              val configName = Paths.get(path).getFileName

              val sparqlIngestStats = statsIngest
                .map { s =>
                  s"""Ingested <span style="color:green"> **${s.ingestedInfotons}** </span> Failed <span style="color:red"> **${s.failedInfotons}** </span>"""
                }
                .getOrElse("")

              val sparqlMaterializerStats = stats
                .get(s"${SparqlTriggeredProcessor.sparqlMaterializerLabel}")
                .map { s =>
                  val totalRunTime = DurationFormatUtils.formatDurationWords(s.runningTime, true, true)
                  val allRunTime = DurationFormatUtils.formatDurationWords(s.totalRunningTime, true, true)
                  s"""Materialized <span style="color:green"> **${s.receivedInfotons}** </span> infotons [$totalRunTime]|/[$allRunTime]""".stripMargin
                }
                .getOrElse("")

              val controls = isAdmin match {
                case true => s"<a href='/_stp/$path?enabled=false'>Pause</a>"
                case false => ""
              }

              Table(title = title :+ sparqlMaterializerStats :+ sparqlIngestStats :+ controls, header = header, body = body)

            }
            case _ => Table(title = title, header = header, body = Seq(Seq("")))
          }
        }
    }

    Future.sequence {
      generateActiveTables(jobs) ++ generateNonActiveTables(jobs)
    }

  }

  /**
    * Gets or creates a reference to a connection Pool Flow
    * for a given destination host
    * @param host
    */
  def getOrCreateConnectionPool(host: Option[String], defaultConnectionPoolHost : String) : Some[ConnectionPool] = {
    connectionPools.get(host.getOrElse(defaultConnectionPoolHost)) match {
      case Some(pool) => Some(pool)
      case _ => {
        val conn = Retry.createNewHostConnectionPool[(Option[StpMetadata], Long)](host.getOrElse(defaultConnectionPoolHost))
        connectionPools += host.getOrElse(defaultConnectionPoolHost) -> conn
        Some(conn)
      }
    }
  }


  /**
    * Gets or creates a reference to a connection Pool Flow
    * for a given destination host
    * @param job
    * @param defaultConnectionPoolHost
    */
  def getOrCreateConnectionPool(job: Job, defaultConnectionPoolHost : String = "localhost") : Some[ConnectionPool] = {
    getOrCreateConnectionPool(job.config.hostUpdatesSource, defaultConnectionPoolHost)
  }

  /**
    * This method MUST be run from the actor's thread (it changes the actor state)!
    * @param jobRead
    */
  def handleStartJob(jobRead: JobRead): Unit = {

    val job = jobRead.job

    //this method MUST BE RUN from the actor's thread and changing the state is allowed. the below will replace any existing state.
    //The state is changed instantly and every change that follows (even from another thread/Future) will be later.
    val tokenReporter = context.actorOf(
      props = InfotonReporter(baseUrl = settings.hostConfigFile, path = settings.pathAgentConfigs + "/" + job.name, zStore = zStore),
      name = s"${job.name}-${Hash.crc32(job.config.toString)}"
    )

    val label = Some(s"ingester-${job.name}")
    val hostUpdatesSource = job.config.hostUpdatesSource.getOrElse(settings.hostUpdatesSource)

    val initialTokensAndStatistics = SparqlTriggeredProcessor.loadInitialTokensAndStatistics(Option(tokenReporter))
    val agentConfig = Await.result(SparqlTriggeredProcessor.preProcessConfig(job.config, Some(tokenReporter)), 3.minutes)

    val sparqlConnectionPool = getOrCreateConnectionPool(job)

    val ingestLoggingFlow = Flow[(Ingester.IngestEvent, Option[StpMetadata])]
      .filter(_._2.get.agentConfig.name.get.equals(job.name)).map(_._1)
      .via(StpPeriodicLogger(infotonReporter = tokenReporter, logFrequency = 5.minutes))
      .via(Ingester.ingestStatsFlow(
        tokenReporterOpt = Some(tokenReporter),
        agentName = label.get,
        initialIngestStatsOpt = initialTokensAndStatistics.fold(_ => None, _.agentIngestStats))
      )

    val consumerSource = SparqlTriggeredProcessor.createSensorSource(agentConfig,
      initialTokensAndStatistics,
      Some(tokenReporter),
      hostUpdatesSource,
      false,
      10.seconds,
      settings.infotonGroupSize)
      .map { case ((path,vars),_) =>  Seq(path) -> vars}
      .asSourceWithContext(_ => Some(StpMetadata(agentConfig,tokenReporter)))
      .via(
        SparqlProcessor.sparqlFlow(extractSparqlContext,
          sparqlConnectionPool,
          spQueryParamsBuilder = SparqlTriggeredProcessor.stpSpQueryBuilder
        )
      )
      .via(
        SparqlProcessor.materializedStatsFlow(Some(tokenReporter),
          format = "ntriples",
          sensorName = SparqlTriggeredProcessor.sparqlMaterializerLabel,
          initialTokensAndStatistics.fold(_ => None, _.materializedStats))
      ).asSource

    val uniqueKillSwitch = consumerSource.watchTermination() { (_, done) =>
      done.onComplete {
        case Success(_) =>
          logger.info(s"job: $job finished successfully")
          //The stream has already finished - kill the token actor
          tokenReporter ! PoisonPill
          self ! JobHasFinished(job)
        case Failure(ex) =>
          logger.error(
            s"job: $job finished with error (In case this job should be running it will be restarted on the next periodic check):",
            ex
          )
          //The stream has already finished - kill the token actor
          tokenReporter ! PoisonPill
          self ! JobHasFailed(job, ex)
      }
    }
    .viaMat(pubSubChannel)(Keep.right)
    .via(ingestLoggingFlow)
    .to(Sink.ignore).run()

    currentJobs = currentJobs + (job.name -> JobRunning(job, uniqueKillSwitch, tokenReporter))
    logger.info(s"starting job $job")

  }


  def getJobConfigsFromTheUser: Future[Set[JobRead]] = {
    logger.info("Checking the current status of the Sparql Triggered Processor manager config infotons")
    //val initialRetryState = RetryParams(2, 2.seconds, 1)
    //retryUntil(initialRetryState)(shouldRetry("Getting config information from local cm-well")) {
    safeFuture(
      client.get(s"http://${settings.hostConfigFile}${settings.pathAgentConfigs}",
        queryParams = List("op" -> "search", "with-data" -> "", "format" -> "json", "length" -> "100"))
    ).map{
      case response@SimpleResponse(respCode,_,_) if respCode < 300 =>
        parseJobsJson(response.payload)
      case failedResponse@SimpleResponse(respCode,_,_) =>
        logger.warn(s"Failed to read config infoton from cm-well. Error code: $respCode payload: ${failedResponse.payload} headers: ${failedResponse.headers}")
        throw new Exception(failedResponse.payload)
    }.andThen {
      case Failure(ex) =>
        logger.warn(
          "Reading the config infotons failed. It will be checked on the next schedule check. The exception was: ",
          ex
        )
    }
  }

  def parseJobsJson(configJson: String): Set[JobRead] = {
    import io.circe._
    import io.circe.parser._
    //This method is run from map of a future - no need for try/catch (=can throw exceptions here). Each exception will be mapped to a failed future.
    val parsedJson = parse(configJson)
    parsedJson match {
      case Left(parseFailure @ ParsingFailure(message, ex)) =>
        logger.error(s"Parsing the agent config files failed with message: $message. Cancelling this configs check. " +
          s"It will be checked on the next iteration. The exception was: ", ex)
        throw parseFailure
      case Right(json) => {
        try {
          val infotons = json.hcursor.downField("results").downField("infotons").values.get
          infotons.view.map { infotonJson =>
            val name = infotonJson.hcursor
              .downField("system")
              .get[String]("path")
              .toOption
              .get
              .drop(settings.pathAgentConfigs.length + 1)
            val configStr = infotonJson.hcursor.downField("content").get[String]("data").toOption.get
            val configRead = yamlToConfig(configStr)
            val modifiedSensors = configRead.sensors.map(sensor => sensor.copy(name = s"$name${sensor.name}"))
            //The active field enables the user to disable the job. In case it isn't there, it's active.
            val active =
              infotonJson.hcursor.downField("fields").downField("active").downArray.as[Boolean].toOption.getOrElse(true)
            JobRead(Job(name, configRead.copy(sensors = modifiedSensors)), active)
          }.to(Set)
        } catch {
          case ex: Throwable =>
            logger.warn(
              s"The manager failed to parse the json of the config infotons failed with exception! The json was: $json"
            )
            throw ex
        }

      }
    }
  }

  def yamlToConfig(yaml: String): Config = {
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
    val yamlConfig = yaml.parseYaml
    yamlConfig.convertTo[Config]
  }

  override def postStop(): Unit = {
    logger.warn(s"${this.getClass.getSimpleName} died, stopping all running jobs")
    configMonitor.cancel()
    currentJobs
      .collect {
        case (_, job: JobRunning) => job
      }
      .foreach { jobRunning =>
        logger.info(s"Stopping job ${jobRunning.job} due to STP manager actor's death")
        jobRunning.killSwitch.shutdown()
      }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"Sparql triggered processor manager died during processing of $message. The exception was: ", reason)
    super.preRestart(reason, message)
  }

  def shouldRetry(action: String): (Try[SimpleResponse[String]], RetryParams) => ShouldRetry[RetryParams] = {
    import scala.language.implicitConversions
    implicit def asFiniteDuration(d: Duration) = scala.concurrent.duration.Duration.fromNanos(d.toNanos);
    {
      //a successful post - don't retry
      case (Success(simpleResponse), _) if simpleResponse.status == 200 => DoNotRetry
      /*
          // Currently, the shouldRetry function is used only to check the config - no need for special 503 treatment
          // is will be retried anyway (and it can cause a bug the a current config check
          // is still running while a new one will be started and a new one and so on...)
          // The "contract" is that with 503 keep retrying indefinitely, but don't spam the server - increase the delay (cap it with a maxDelay)
          case (Success(SimpleResponse(status, headers, body)), state) if status == 503 => {
            logger.warn(s"$action failed. Cm-Well returned bad response: status: $status headers: ${StpUtil.headersString(headers)}" +
                        s"body: $body. Will retry indefinitely on this error.")
            RetryWith(state.copy(delay = (state.delay * state.delayFactor).min(settings.maxDelay)))
          }
       */
      case (Success(SimpleResponse(status, headers, body)), state) if state.retriesLeft == 0 => {
        val h = StpUtil.headersString(headers)
        logger.error(s"$action failed. Cm-Well returned bad response: status: $status headers: $h body: $body. No more retries left, will fail the request!")
        DoNotRetry
      }
      case (Success(SimpleResponse(status, headers, body)), state @ RetryParams(retriesLeft, delay, delayFactor)) => {
        val newDelay = delay * delayFactor
        logger.warn(s"$action failed. Cm-Well returned bad response: status: $status headers: ${StpUtil
          .headersString(headers)} body: $body. $retriesLeft retries left. Will retry in $newDelay")
        RetryWith(state.copy(delay = newDelay, retriesLeft = retriesLeft - 1))
      }
      case (Failure(ex), state) if state.retriesLeft == 0 => {
        logger.error(
          s"$action failed. The HTTP request failed with an exception. No more retries left, will fail the request! The exception was: ",
          ex
        )
        DoNotRetry
      }
      case (Failure(ex), state @ RetryParams(retriesLeft, delay, delayFactor)) => {
        val newDelay = delay * delayFactor
        logger.warn(
          s"$action failed. The HTTP request failed with an exception. $retriesLeft retries left. Will retry in $newDelay. The exception was: ",
          ex
        )
        RetryWith(state.copy(delay = newDelay, retriesLeft = retriesLeft - 1))
      }
    }
  }
}
