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
import java.time.{Instant, LocalDateTime, ZoneId}

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, PoisonPill, Status}
import akka.pattern._
import akka.stream._
import akka.stream.scaladsl._
import akka.util.Timeout
import cmwell.ctrl.checkers.StpChecker.{RequestStats, ResponseStats, Row, Table}
import cmwell.tools.data.ingester._
import cmwell.tools.data.sparql.InfotonReporter.{RequestDownloadStats, RequestIngestStats, ResponseDownloadStats, ResponseIngestStats}
import cmwell.tools.data.sparql.SparqlProcessorManager._
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.akka.stats.IngesterStats
import cmwell.tools.data.utils.chunkers.GroupChunker
import cmwell.tools.data.utils.chunkers.GroupChunker._
import cmwell.tools.data.utils.text.Tokens
import cmwell.util.http.SimpleResponse
import cmwell.util.string.Hash
import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
import cmwell.util.concurrent._
import com.typesafe.scalalogging.LazyLogging
import k.grid.GridReceives
import net.jcazevedo.moultingyaml._
import org.apache.commons.lang3.time.DurationFormatUtils
import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success, Try}
import ExecutionContext.Implicits.global

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

  //todo: should we use injected ec??? implicit val ec = context.dispatcher
  implicit val system: ActorSystem = context.system
  implicit val mat = ActorMaterializer()

  if (mat.isInstanceOf[ActorMaterializer]) {
    require(mat.asInstanceOf[ActorMaterializer].system eq context.system,
            "ActorSystem of materializer MUST be the same as the one used to create current actor")
  }

  var currentJobs: Jobs = Map.empty

  var configMonitor: Cancellable = _

  override def preStart(): Unit = {
    logger.info("starting sparql-processor manager instance on this machine")
    configMonitor = context.system.scheduler.schedule(settings.initDelay, settings.interval, self, CheckConfig)
  }

  override def receive: Receive = {
    import akka.pattern._
    GridReceives.monitoring(sender).orElse {
      case RequestStats                          => stringifyActiveJobs(currentJobs).map(ResponseStats.apply).pipeTo(sender())
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
  def stringifyActiveJobs(jobs: Jobs): Future[Iterable[Table]] = {
    implicit val timeout = Timeout(1.minute)

    def generateNonActiveTables(jobs: Jobs) = jobs.collect {
      case (path, jobStatus @ (_: JobPaused | _: JobFailed)) =>
        val (colour, status) = jobStatus match {
          case jobFailed @ (_: JobFailed) =>
            ("red", "Exception : " + jobFailed.ex.getMessage)
          case _: JobPaused =>
            ("green", "No exceptions reported")
        }

        val sensorNames = jobStatus.job.config.sensors.map(_.name)
        val title = Seq(
          s"""<span style="color:${colour}"> **Non-Active - ${jobStatus.statusString} ** </span> ${path} <br/><span style="color:${colour}">${status}</span>"""
        )
        val header = Seq("Sensor", "Token Time")

        StpUtil.readPreviousTokens(settings.hostConfigFile, settings.pathAgentConfigs + "/" + path, "ntriples").map {
          result =>
            result match {
              case Right(storedTokens) =>
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
                Table(title = title, header = header, body = body)
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
            case Right(storedTokens) => {

              val sensorNames = jobConfig.sensors.map(_.name)
              val pathsWithoutSavedToken = sensorNames.toSet.diff(storedTokens.keySet)
              val allSensorsWithTokens = storedTokens ++ pathsWithoutSavedToken.map(_ -> ("", None))

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
                        case 0 => "Not Yet Updated"
                        case _ =>
                          LocalDateTime.ofInstant(Instant.ofEpochMilli(s.statsTime), ZoneId.systemDefault()).toString
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
                .get(s"ingester-$configName")
                .map { s =>
                  s"""Ingested <span style="color:green"> **${s.ingestedInfotons}** </span> Failed <span style="color:red"> **${s.failedInfotons}** </span>"""
                }
                .getOrElse("")

              val sparqlMaterializerStats = stats
                .get(s"$configName-${SparqlTriggeredProcessor.sparqlMaterializerLabel}")
                .map { s =>
                  val totalRunTime = DurationFormatUtils.formatDurationWords(s.runningTime, true, true)
                  s"""Materialized <span style="color:green"> **${s.receivedInfotons}** </span> infotons [$totalRunTime]""".stripMargin
                }
                .getOrElse("")

              Table(title = title :+ sparqlMaterializerStats :+ sparqlIngestStats, header = header, body = body)

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
    * This method MUST be run from the actor's thread (it changes the actor state)!
    * @param jobRead
    */
  def handleStartJob(jobRead: JobRead): Unit = {

    val job = jobRead.job

    //this method MUST BE RUN from the actor's thread and changing the state is allowed. the below will replace any existing state.
    //The state is changed instantly and every change that follows (even from another thread/Future) will be later.
    val tokenReporter = context.actorOf(
      props = InfotonReporter(baseUrl = settings.hostConfigFile, path = settings.pathAgentConfigs + "/" + job.name),
      name = s"${job.name}-${Hash.crc32(job.config.toString)}"
    )

    val label = Some(s"ingester-${job.name}")

    val hostUpdatesSource = job.config.hostUpdatesSource.getOrElse(settings.hostUpdatesSource)

    val agent = SparqlTriggeredProcessor
      .listen(job.config, hostUpdatesSource, false, Some(tokenReporter), Some(job.name), infotonGroupSize = settings.infotonGroupSize)
      .map { case (data, _) => data }
      .via(GroupChunker(formatToGroupExtractor(settings.materializedViewFormat)))
      .map(concatByteStrings(_, endl))
    val (killSwitch, jobDone) = Ingester
      .ingest(
        baseUrl = settings.hostWriteOutput,
        format = settings.materializedViewFormat,
        source = agent,
        writeToken = Option(settings.writeToken),
        force = job.config.force.getOrElse(false),
        label = label
      )
      .via(IngesterStats(isStderr = false, reporter = Some(tokenReporter), label = label))
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()
    currentJobs = currentJobs + (job.name -> JobRunning(job, killSwitch, tokenReporter))
    logger.info(s"starting job $job")
    jobDone.onComplete {
      case Success(_) => {
        logger.info(s"job: $job finished successfully")
        //The stream has already finished - kill the token actor
        tokenReporter ! PoisonPill
        self ! JobHasFinished(job)
      }
      case Failure(ex) => {
        logger.error(
          s"job: $job finished with error (In case this job should be running it will be restarted on the next periodic check):",
          ex
        )
        //The stream has already finished - kill the token actor
        tokenReporter ! PoisonPill
        self ! JobHasFailed(job, ex)
      }
    }
  }

  def getJobConfigsFromTheUser: Future[Set[JobRead]] = {
    logger.info("Checking the current status of the Sparql Triggered Processor manager config infotons")
    //val initialRetryState = RetryParams(2, 2.seconds, 1)
    //retryUntil(initialRetryState)(shouldRetry("Getting config information from local cm-well")) {
    safeFuture(
      client.get(s"http://${settings.hostConfigFile}${settings.pathAgentConfigs}",
                 queryParams = List("op" -> "search", "with-data" -> "", "format" -> "json"))
    ).map(response => parseJobsJson(response.payload)).andThen {
      case Failure(ex) =>
        logger.warn(
          "Reading the config infotons failed. It will be checked on the next schedule check. The exception was: ",
          ex
        )
    }
  }

  private[this] val parsedJsonsBreakOut = scala.collection.breakOut[Iterable[Json], JobRead, Set[JobRead]]
  def parseJobsJson(configJson: String): Set[JobRead] = {
    import cats.syntax.either._
    import io.circe._, io.circe.parser._
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
          infotons.map { infotonJson =>
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
          }(parsedJsonsBreakOut)
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
      implicit val configFormat = yamlFormat6(Config)

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
