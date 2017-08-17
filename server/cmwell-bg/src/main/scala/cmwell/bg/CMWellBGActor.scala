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


package cmwell.bg

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props}
import akka.kafka.{ConsumerSettings, KafkaConsumerActor}
import akka.stream.{ActorMaterializer, Supervision}
import cmwell.fts.FTSServiceNew
import cmwell.irw.IRWService
import cmwell.common.OffsetsService
import cmwell.common.ExitWithError
import cmwell.zstore.ZStore
import com.codahale.metrics.JmxReporter
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.DefaultInstrumented
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.elasticsearch.metrics.ElasticsearchReporter

import scala.concurrent.duration._




object CMWellBGActor {
  val name = "CMWellBGActor"
  def props(partition:Int, config:Config, irwService:IRWService, ftsService:FTSServiceNew, zStore: ZStore,
            offsetsService: OffsetsService) =
    Props(new CMWellBGActor(partition, config, irwService, ftsService, zStore, offsetsService))
}

/**
  * Created by israel on 15/06/2016.
  */
class CMWellBGActor(partition:Int, config:Config, irwService:IRWService, ftsService:FTSServiceNew, zStore: ZStore,
                    offsetsService: OffsetsService) extends Actor with LazyLogging with DefaultInstrumented {

  var impStream:ImpStream = null
  var indexerStream:IndexerStream = null
  val waitAfter503 = config.getInt("cmwell.bg.waitAfter503")

  // Metrics
  val jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
  jmxReporter.start()
  val reportMetricsToES = config.getBoolean("cmwell.common.reportMetricsToES")
    logger debug (s"report to es set to $reportMetricsToES")
  val esReporterOpt:Option[ElasticsearchReporter] = if(reportMetricsToES) {
      logger debug (s"available ES nodes: ${ftsService.nodesHttpAddresses().mkString(",")}")
    Some(ElasticsearchReporter.forRegistry(metricRegistry).hosts(ftsService.nodesHttpAddresses() :_*).build())
  } else None

  esReporterOpt.foreach{ esReporter =>
    logger info "starting metrics ES Reporter"
    esReporter.start(10, TimeUnit.SECONDS)
  }

  val bootStrapServers = config.getString("cmwell.bg.kafka.bootstrap.servers")

  val byteArrayDeserializer = new ByteArrayDeserializer()
  val persistCommandsConsumerSettings =
    ConsumerSettings(context.system, byteArrayDeserializer, byteArrayDeserializer)
      .withBootstrapServers(bootStrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val kafkaConsumer: ActorRef = context.actorOf(KafkaConsumerActor.props(persistCommandsConsumerSettings))

  override def preStart(): Unit = {
      logger info s"CMwellBGActor-$partition starting"
    super.preStart()
    self ! Start
  }


  override def postStop(): Unit = {
      logger info s"CMWellBGActor-$partition stopping"
    esReporterOpt.foreach(_.close())
    stopAll
    super.postStop()
  }

  override def supervisorStrategy = OneForOneStrategy(){
    case t:Throwable =>
      logger error ("Exception caught in supervisor. resuming children actors", t)
      akka.actor.SupervisorStrategy.Resume
  }

  object ExceptionOrigin {
    val CassandraException = ExceptionOrigin("com.datastax.driver.core.exceptions")
    val ElasticSearchException = ExceptionOrigin("java.lang.IllegalArgumentException")
  }
  case class ExceptionOrigin(exceptionPrefix:String) {
    def unapply(t:Throwable): Boolean = t.getClass.getName.startsWith(exceptionPrefix)
  }

  val decider: Supervision.Decider = {
    case ExceptionOrigin.CassandraException() =>
      self ! All503
      Supervision.Stop
    case ExceptionOrigin.ElasticSearchException() =>
      self ! All503
      Supervision.Stop
    case t: Throwable =>
      if(Option(t.getMessage).getOrElse("").contains("""Sender[null] sent the message of type "akka.kafka.KafkaConsumerActor$Internal$Commit"""")){
        logger error s"got the weired exception from reactive kafka !!! restarting stream"
        Supervision.Restart
      } else {
        logger error ("Got exception from stream. suspending all sending All503 to myself", t)
        self ! All503
        Supervision.Stop
      }
  }

  implicit val system = context.system

  implicit val ec = context.dispatcher

  implicit val materializer = ActorMaterializer()


  override def receive: Receive = {
    case Start =>
        logger info "requested to start all streams"
      startAll
      sender() ! Started
    case StartImp =>
        logger info "requested to start Imp Stream"
      startImp
      sender() ! Started
    case StartIndexer =>
        logger info "requested to start Indexer Stream"
      startIndexer
      sender() ! Started
    case Stop =>
        logger info "requested to stop all streams"
      stopAll
      sender() ! Stopped
    case StopImp =>
        logger info "requested to stop Imp Stream"
      stopImp
      sender() ! Stopped
    case StopIndexer =>
        logger info "requested to stop Indexer Stream"
      stopIndexer
      sender() ! Stopped
    case ShutDown =>
        logger info "requested to shutdown"
      stopAll
        logger info "stopped all streams. taking the last pill...."
      self ! PoisonPill
    case All503 =>
        logger info "Got all503 message. becoming state503"
      context become state503
        logger debug "stopping all streams"
      stopAll
        logger debug s"became state503. scheduling resume in [waitAfter503] seconds"
      context.system.scheduler.scheduleOnce(waitAfter503.seconds, self, Resume)
    case Indexer503 =>
        logger info "Indexer went offline temporarily, will try to start it again in 30 seconds"
      stopIndexer
      context become state503
      context.system.scheduler.scheduleOnce(5.seconds, self, ResumeIndexer)
    case ExitWithError =>
      logger error s"Requested to exit with error by ${sender()}"
      System.exit(1)
  }

  def state503: Receive = {
    case Resume =>
      logger info "accepted Resume message"
      context become receive
      logger info s"became normal and sending Start message to myself"
      self ! Start

    case ResumeIndexer =>
      self ! StartIndexer
      context become receive

    case ShutDown =>
      logger info "requested to shutdown"
      stopAll
      logger info "stopped all streams. taking the last pill...."
      self ! PoisonPill

    case ExitWithError =>
      logger error s"Requested to exit with error by ${sender()}"
      System.exit(1)

    case x => logger debug s"got $x in state503 state, ignoring!!!!"
  }

  def shutdown = {
    indexerStream.shutdown
    impStream.shutdown
  }

  private def startImp = {
    if(impStream == null) {
      impStream = new ImpStream(partition, config, irwService, zStore, ftsService, offsetsService, decider, kafkaConsumer)
      logger info "Imp Stream started"
    } else
      logger warn "requested to start Imp Stream but it is already running. doing nothing."
  }

  private def startIndexer = {
    if(indexerStream == null) {
      indexerStream = new IndexerStream(partition, config, irwService, ftsService, offsetsService, decider, kafkaConsumer)
      logger info "Indexer Stream started"
    } else
      logger warn "requested to start Indexer Stream but it is already running. doing nothing."
  }

  private def startAll = {
    startImp
    startIndexer
  }

  /**
    * Stop the Imp Stream. If already running, will do nothing
    */
  private def stopImp = {
    if(impStream != null) {
      impStream.shutdown
      logger info "stopped imp stream"
    } else
      logger info "Imp Stream was already stopped"
    impStream = null
  }

  private def stopIndexer = {
    if(indexerStream != null ) {
      indexerStream.shutdown
      logger info "stopped Indexer Stream"
    } else
      logger info "Indexer Stream was already stopped"
    indexerStream = null
  }

  private def stopAll = {
    stopIndexer
    stopImp
  }

}

case object Start
case object Started
case object StartImp
case object ImpStarted
case object StartIndexer
case object IndexerStarted
case object Stop
case object Stopped
case object StopImp
case object ImpStopped
case object StopIndexer
case object IndexerStopped
case object ShutDown
case object All503
case object Indexer503
case object State503
case object Resume
case object ResumeIndexer
case object Suspend





trait ESIndicesMapping {
  /**
    * gets relevant indices for given Infoton's UUID
    */
  def indicesForUuid(uuid:String):Iterable[String]
}

class SimpleESIndicesMapping(mapping:Map[String, Iterable[String]]) extends ESIndicesMapping {
  /**
    * gets relevant indices for given Infoton's UUID
    */
  override def indicesForUuid(uuid: String): Iterable[String] = mapping.get(uuid).getOrElse(Iterable.empty)
}

object BGIdentifiedException {
  val IRWRelated = BGIdentifiedException("com.datastax.driver.core.exceptions")
  val FTSRelated = BGIdentifiedException("org.elasticsearch")
}

case class BGIdentifiedException(prefix:String) {
  def unapply(t: Throwable): Boolean = t.getClass.getName.startsWith(prefix)
}
