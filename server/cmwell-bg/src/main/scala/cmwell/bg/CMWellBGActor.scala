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
package cmwell.bg

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props}
import akka.kafka.{ConsumerSettings, KafkaConsumerActor}
import akka.stream.{ActorMaterializer, Supervision}
import ch.qos.logback.classic.LoggerContext
import cmwell.bg.Runner.logger
import cmwell.fts.FTSService
import cmwell.irw.IRWService
import cmwell.common.OffsetsService
import cmwell.common.ExitWithError
import cmwell.zstore.ZStore
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.DefaultInstrumented
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.elasticsearch.metrics.ElasticsearchReporter
import cmwell.common.exception._
import cmwell.crawler.CrawlerStream
import cmwell.crawler.CrawlerStream.CrawlerMaterialization
import com.codahale.metrics.jmx.JmxReporter
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object CMWellBGActor {
  val name = "CMWellBGActor"
  def props(partition:Int, config:Config, irwService:IRWService, ftsService:FTSService, zStore: ZStore,
            offsetsService: OffsetsService) =
    Props(new CMWellBGActor(partition, config, irwService, ftsService, zStore, offsetsService))
}

/**
  * Created by israel on 15/06/2016.
  */
class CMWellBGActor(partition:Int, config:Config, irwService:IRWService, ftsService:FTSService, zStore: ZStore,
                    offsetsService: OffsetsService) extends Actor with LazyLogging with DefaultInstrumented {

  var impStream: ImpStream = null
  var indexerStream: IndexerStream = null
  val waitAfter503 = config.getInt("cmwell.bg.waitAfter503")
  val crawlerRestartDelayTime = config.getDuration("cmwell.crawler.restartDelayTime").toMillis.millis
  val impOn = config.getBoolean("cmwell.bg.ImpOn")
  val indexerOn = config.getBoolean("cmwell.bg.IndexerOn")
  val persistCommandsTopic = config.getString("cmwell.bg.persist.commands.topic")
  val persistCommandsTopicPriority = persistCommandsTopic + ".priority"
  val indexCommandsTopic = config.getString("cmwell.bg.index.commands.topic")
  val indexCommandsTopicPriority = indexCommandsTopic + ".priority"
  val crawlerMaterializations: scala.collection.mutable.Map[String, CrawlerMaterialization] =
    scala.collection.mutable.Map(persistCommandsTopic -> null, persistCommandsTopicPriority -> null)

  // Metrics
  val bgMetrics = new BGMetrics
  val jmxReporter = JmxReporter.forRegistry(bgMetrics.metricRegistry).build()
  jmxReporter.start()
  val reportMetricsToES = config.getBoolean("cmwell.common.reportMetricsToES")
  logger.debug(s"report to es set to $reportMetricsToES")
  val esReporterOpt: Option[ElasticsearchReporter] = if (reportMetricsToES) {
    logger.debug(s"available ES nodes: ${ftsService.nodesHttpAddresses().mkString(",")}")
    Some(ElasticsearchReporter.forRegistry(metricRegistry).hosts(ftsService.nodesHttpAddresses(): _*).build())
  } else None

  esReporterOpt.foreach { esReporter =>
    logger.info("starting metrics ES Reporter")
    esReporter.start(10, TimeUnit.SECONDS)
  }

  override def preStart(): Unit = {
    logger.info(s"CMwellBGActor-$partition starting")
    super.preStart()
    self ! Start
  }

  override def postStop(): Unit = {
    logger.info(s"CMWellBGActor-$partition stopping")
    esReporterOpt.foreach(_.close())
    stopAll
    super.postStop()
  }

  override def supervisorStrategy = OneForOneStrategy() {
    case t: Throwable =>
      logger.error("Exception caught in supervisor. resuming children actors", t)
      akka.actor.SupervisorStrategy.Resume
  }

  implicit val system = context.system

  implicit val ec = context.dispatcher

  implicit val materializer = ActorMaterializer()
  override def receive: Receive = {
    case Start =>
      logger.info("requested to start all streams")
      startAll
//      sender() ! Started
//    case StartImp =>
//      logger.info("requested to start Imp Stream")
//      startImp
//      sender() ! Started
//    case StartIndexer =>
//      logger.info("requested to start Indexer Stream")
//      startIndexer
//      sender() ! Started
//    case Stop =>
//      logger.info("requested to stop all streams")
//      stopAll
//      sender() ! Stopped
//    case StopImp =>
//      logger.info("requested to stop Imp Stream")
//      stopImp
//      sender() ! Stopped
//    case StopIndexer =>
//      logger.info("requested to stop Indexer Stream")
//      stopIndexer
//      sender() ! Stopped
    case ShutDown =>
      logger.info("requested to shutdown")
      stopAll
      logger.info("stopped all streams. taking the last pill....")
      sender ! BgKilled
      self ! PoisonPill

//    case All503 =>
//      logger.info("Got all503 message. becoming state503")
//      context.become(state503)
//      logger.debug("stopping all streams")
//      stopAll
//      logger.debug(s"became state503. scheduling resume in [waitAfter503] seconds")
//      context.system.scheduler.scheduleOnce(waitAfter503.seconds, self, Resume)
    case MarkCrawlerAsStopped(topic) =>
      logger.info(s"Crawler [$topic, partition: $partition] stopped. Check its logs for details. " +
        s"Marking it as stopped and restarting it in $crawlerRestartDelayTime")
      crawlerMaterializations(topic) = null
      system.scheduler.scheduleOnce(crawlerRestartDelayTime)(self ! StartCrawler(topic))
    case StartCrawler(topic) =>
      startCrawler(topic)
    case Indexer503 =>
      logger.error("Indexer Stopped with Exception. check indexer log for details. Restarting indexer.")
      stopIndexer
      startIndexer
    case Imp503 =>
      logger.error("Imp stopped with exception. check imp log for details. Restarting imp.")
      stopImp
      startImp
    case ExitWithError =>
      logger.error(s"Requested to exit with error by ${sender()}")
      System.exit(1)
  }

//  def state503: Receive = {
////    case Resume =>
////      logger.info("accepted Resume message")
////      context.become(receive)
////      logger.info(s"became normal and sending Start message to myself")
////      self ! Start
//
////    case ResumeIndexer =>
////      self ! StartIndexer
////      context.become(receive)
//
//    case ShutDown =>
//      logger.info("requested to shutdown")
//      stopAll
//      logger.info("stopped all streams. taking the last pill....")
//      self ! PoisonPill
//
//    case ExitWithError =>
//      logger.error(s"Requested to exit with error by ${sender()}")
//      System.exit(1)
//
//    case x => logger.debug(s"got $x in state503 state, ignoring!!!!")
//  }

  def shutdown = {
    indexerStream.shutdown
    impStream.shutdown
  }

  private def startImp = {
    if (impOn) {
      if (impStream == null) {
        logger.info("starting ImpStream")
        impStream = new ImpStream(partition, config, irwService, zStore, ftsService, offsetsService, self, bgMetrics)
      } else
        logger.warn("requested to start Imp Stream but it is already running. doing nothing.")
    }
  }

  private def startIndexer = {
    if (indexerOn) {
      if (indexerStream == null) {
        logger.info("starting IndexerStream")
        indexerStream = new IndexerStream(partition, config, irwService, ftsService, zStore, offsetsService, self)
      } else
        logger.warn("requested to start Indexer Stream but it is already running. doing nothing.")
    }
  }

  private def startCrawler(topic: String) = {
    if (crawlerMaterializations(topic) == null) {
      logger.info(s"starting Crawler [$topic, partition: $partition]")
      crawlerMaterializations(topic) = CrawlerStream.createAndRunCrawlerStream(config, topic, partition)(
        irwService, ftsService, zStore, offsetsService)(system, materializer, ec)
      crawlerMaterializations(topic).doneState.onComplete {
        case Success(_) =>
          logger.info(s"Crawler [$topic, partition: $partition] finished with success. Sending a self message to mark it as finished.")
          self ! MarkCrawlerAsStopped(topic)
        case Failure(ex) =>
          logger.error(s"Crawler [$topic, partition: $partition] finished with exception. " +
            s"Sending a self message to mark it as finished. The exception was: ", ex)
          self ! MarkCrawlerAsStopped(topic)
      }
      //The stream didn't even start - set it as null
      if (crawlerMaterializations(topic).control == null)
        crawlerMaterializations(topic) = null
    } else
      logger.error(s"It was requested to start Crawler [$topic, partition: $partition] but it is already running. doing nothing.")
  }


  private def stopCrawler(topic: String) = {
    if (crawlerMaterializations(topic) != null) {
      logger.info(s"Sending the stop signal to Crawler [$topic, partition: $partition]")
      val res = crawlerMaterializations(topic).control.shutdown()
      res.onComplete {
        case Success(_) => logger.info(s"The future of the crawler stream shutdown control of Crawler [$topic, partition: $partition] " +
          s"finished with success. It will be marked as stopped only after the stream will totally finish.")
        case Failure(ex) => logger.error(s"The future of the crawler stream shutdown control of Crawler [$topic, partition: $partition] " +
          s"finished with exception. The crawler stream will be marked as stopped only after the stream will totally finish.The exception was: ", ex)
      }
    } else
      logger.error(s"Crawler [$topic, partition: $partition] was already stopped and it was requested to finish it again. Not reasonable!")
  }


  private def checkPersistencyAndUpdateIfNeeded(): Unit = {
    val bootStrapServers = config.getString("cmwell.bg.kafka.bootstrap.servers")
    def checkAndFix(streamType: String, topic: String, topicPriority: String) = {
      val streamId = s"$streamType.$partition"
      val offsetId = s"${streamId}_offset"
      val persistedOffset = offsetsService.read(offsetId).getOrElse(0L)
      val offsetIdPriority = s"$streamId.p_offset"
      val persistedOffsetPriority = offsetsService.read(offsetIdPriority).getOrElse(0L)
      val (earliestOffset, latestOffset) = OffsetUtils.getOffsetBoundries(bootStrapServers, new TopicPartition(topic, partition))
      val (earliestOffsetPriority, latestOffsetPriority) = OffsetUtils.getOffsetBoundries(bootStrapServers, new TopicPartition(topicPriority, partition))
      logger.info(s"Persisted $streamType offsets [normal, priority] for partition $partition are [$persistedOffset, $persistedOffsetPriority]")
      logger.info(s"Earliest $streamType offsets in kafka topics [$topic, $topicPriority] for partition $partition " +
        s"are [$earliestOffset, $earliestOffsetPriority]")
      logger.info(s"Latest $streamType offsets in kafka topics [$topic, $topicPriority] for partition $partition " +
        s"are [$latestOffset, $latestOffsetPriority]")
      if (persistedOffset < earliestOffset || persistedOffset > latestOffset) {
        logger.error(s"Persisted offset $persistedOffset for [$topic, partition: $partition] is out of range. " +
          s"Setting it to the earliest available offset [$earliestOffset]. It probably means data loss!")
        Await.result(offsetsService.writeAsync(offsetId, earliestOffset), 10.seconds)
      }
      if (persistedOffsetPriority < earliestOffsetPriority || persistedOffsetPriority > latestOffsetPriority) {
        logger.error(s"Persisted offset $persistedOffsetPriority for [$topic, partition: $partition] is out of range. " +
          s"Setting it to the earliest available offset [$earliestOffsetPriority]. It probably means data loss!")
        Await.result(offsetsService.writeAsync(offsetIdPriority, earliestOffsetPriority), 10.seconds)
      }
    }
    checkAndFix("imp", persistCommandsTopic, persistCommandsTopicPriority)
    checkAndFix("indexer", indexCommandsTopic, indexCommandsTopicPriority)
  }

  private def startAll = {
    checkPersistencyAndUpdateIfNeeded()
    startCrawler(persistCommandsTopic)
    startCrawler(persistCommandsTopicPriority)
    startImp
    startIndexer
  }

  /**
    * Stop the Imp Stream. If already running, will do nothing
    */
  private def stopImp = {
    if (impStream != null) {
      impStream.shutdown
      logger.info("stopped imp stream")
    } else
      logger.info("Imp Stream was already stopped")
    impStream = null
  }

  private def stopIndexer = {
    if (indexerStream != null) {
      indexerStream.shutdown
      logger.info("stopped Indexer Stream")
    } else
      logger.info("Indexer Stream was already stopped")
    indexerStream = null
  }

  private def stopAll = {
    stopCrawler(persistCommandsTopic)
    stopCrawler(persistCommandsTopicPriority)
    stopIndexer
    stopImp
  }
}

case object Start
//case object Started
//case object StartImp
//case object ImpStarted
//case object StartIndexer
//case object IndexerStarted
//case object Stop
//case object Stopped
//case object StopImp
//case object ImpStopped
//case object StopIndexer
//case object IndexerStopped
case object ShutDown
case object BgKilled
case class MarkCrawlerAsStopped(topic: String)
case class StartCrawler(topic: String)
//case object All503
case object Indexer503
case object Imp503
//case object State503
//case object Resume
//case object ResumeIndexer
//case object Suspend

trait ESIndicesMapping {

  /**
    * gets relevant indices for given Infoton's UUID
    */
  def indicesForUuid(uuid: String): Iterable[String]
}

class SimpleESIndicesMapping(mapping: Map[String, Iterable[String]]) extends ESIndicesMapping {

  /**
    * gets relevant indices for given Infoton's UUID
    */
  override def indicesForUuid(uuid: String): Iterable[String] = mapping.get(uuid).getOrElse(Iterable.empty)
}

object BGIdentifiedException {
  val IRWRelated = BGIdentifiedException("com.datastax.driver.core.exceptions")
  val FTSRelated = BGIdentifiedException("org.elasticsearch")
}

case class BGIdentifiedException(prefix: String) {
  def unapply(t: Throwable): Boolean = t.getClass.getName.startsWith(prefix)
}
