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

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, MergePreferred, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape, KillSwitches, Supervision}
import cmwell.common.formats.JsonSerializerForES
import cmwell.fts._
import cmwell.irw.{IRWService, QUORUM}
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.common._
import cmwell.common.exception.getStackTrace
import cmwell.domain.Infoton
import cmwell.tracking._
import com.typesafe.config.Config
import com.typesafe.scalalogging.{LazyLogging, Logger}
import nl.grons.metrics4.scala.{Counter, DefaultInstrumented, Histogram, Timer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory
import com.codahale.metrics.{Counter => DropwizardCounter, Histogram => DropwizardHistogram, Timer => DropwizardTimer}
import org.apache.kafka.clients.consumer.ConsumerConfig

import collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by israel on 14/06/2016.
  */
class IndexerStream(partition: Int, config: Config, irwService: IRWService, ftsService: FTSServiceNew,
                    offsetsService: OffsetsService, bgActor:ActorRef)
                   (implicit actorSystem:ActorSystem,
                    executionContext:ExecutionContext,
                    materializer: ActorMaterializer) extends DefaultInstrumented{

  implicit val logger = Logger[IndexerStream]

  lazy val redlog = LoggerFactory.getLogger("bg_red_log")
  lazy val heartbitLogger = LoggerFactory.getLogger("heartbeat_log")

  val byteArrayDeserializer = new ByteArrayDeserializer()
  val bootStrapServers = config.getString("cmwell.bg.kafka.bootstrap.servers")
  val indexCommandsTopic = config.getString("cmwell.bg.index.commands.topic")
  val priorityIndexCommandsTopic = indexCommandsTopic + ".priority"
  val offsetFilesDir = config.getString("cmwell.bg.offset.files.dir")
  val latestIndexAliasName = config.getString("cmwell.bg.latestIndexAliasName")
  val allIndicesAliasName = config.getString("cmwell.bg.allIndicesAliasName")
  val numOfCassandraNodes = config.getInt("cmwell.bg.num.of.cassandra.nodes")
  val maxAggregatedWeight = config.getInt("cmwell.bg.maxAggWeight")
  val esActionsBulkSize = config.getInt("cmwell.bg.esActionsBulkSize") // in bytes
  val esActionsGroupingTtl = config.getInt("cmwell.bg.esActionsGroupingTtl") // ttl for bulk es actions grouping in ms

  /***** Metrics *****/
  val existingMetrics = metricRegistry.getMetrics.asScala
  val indexNewInfotonCommandCounter:Counter = existingMetrics.get("IndexNewCommand Counter").
    map{ m => new Counter(m.asInstanceOf[DropwizardCounter])}.
    getOrElse(metrics.counter("IndexNewCommand Counter"))
  val indexExistingCommandCounter:Counter = existingMetrics.get("IndexExistingCommand Counter").
    map{ m => new Counter(m.asInstanceOf[DropwizardCounter])}.
    getOrElse(metrics.counter("IndexExistingCommand Counter"))
  val indexingTimer:Timer = existingMetrics.get("Indexing Timer").map{ m => new Timer(m.asInstanceOf[DropwizardTimer])}.
    getOrElse(metrics.timer("Indexing Timer"))
  val indexBulkSizeHist:Histogram = existingMetrics.get("Index Bulk Size Histogram").
    map{ m => new Histogram(m.asInstanceOf[DropwizardHistogram])}.
    getOrElse(metrics.histogram("Index Bulk Size Histogram"))

  /*******************/

  val streamId = s"indexer.${partition}"

  val startingOffset = offsetsService.read(s"${streamId}_offset").getOrElse(0L)
  val startingOffsetPriority = offsetsService.read(s"${streamId}.p_offset").getOrElse(0L)

    logger info s"IndexerStream($streamId), startingOffset: $startingOffset, startingOffsetPriority: $startingOffsetPriority"

  val subscription = Subscriptions.assignmentWithOffset(
    new TopicPartition(indexCommandsTopic, partition) -> startingOffset)

  val prioritySubscription = Subscriptions.assignmentWithOffset(
    new TopicPartition(priorityIndexCommandsTopic, partition) -> startingOffsetPriority)

  val indexCommandsConsumerSettings =
    ConsumerSettings(actorSystem, byteArrayDeserializer, byteArrayDeserializer)
      .withBootstrapServers(bootStrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val sharedKillSwitch = KillSwitches.shared("indexer-sources-kill-switch")

  val indexCommandsSource = Consumer.plainSource[Array[Byte], Array[Byte]](indexCommandsConsumerSettings, subscription).map { msg =>
      logger debug s"consuming next payload from index commands topic @ offset: ${msg.offset()}"
    val indexCommand = CommandSerializer.decode(msg.value()).asInstanceOf[IndexCommand]
      logger debug s"converted payload to an IndexCommand:\n$indexCommand"
    BGMessage[IndexCommand](CompleteOffset(msg.topic(), msg.offset()), indexCommand)
  }.via(sharedKillSwitch.flow)

  val priorityIndexCommandsSource = Consumer.plainSource[Array[Byte], Array[Byte]](indexCommandsConsumerSettings, prioritySubscription).map { msg =>
    logger debug s"consuming next payload from priority index commands topic @ offset: ${msg.offset()}"
    val indexCommand = CommandSerializer.decode(msg.value()).asInstanceOf[IndexCommand]
    logger debug s"converted priority payload to an IndexCommand:\n$indexCommand"
    BGMessage[IndexCommand](CompleteOffset(msg.topic(), msg.offset()), indexCommand)
  }.via(sharedKillSwitch.flow)

  val heartBitLog = Flow[BGMessage[IndexCommand]].keepAlive(60.seconds, () => BGMessage(HeartbitCommand.asInstanceOf[Command])).filterNot{
    case BGMessage(_, HeartbitCommand) =>
      heartbitLogger info "Indexer alive !!!"
      true
    case _ => false
  }.map{x => x.asInstanceOf[BGMessage[IndexCommand]]}

  import scala.language.existentials

  case class InfoAction(esAction:ActionRequest[ _ <: ActionRequest[_ <: AnyRef]], weight:Long, indexTime:Option[Long])


  val commitOffsets = Flow[Seq[Offset]].groupedWithin(6000, 3.seconds).toMat{
    Sink.foreach{ offsetGroups =>
      val (offsets, offsetsPriority) = offsetGroups.flatten.partition(_.topic == indexCommandsTopic)
      if(offsets.length >0) {
        val lastOffset = offsets.map(_.offset).max
          logger debug s"committing last offset: $lastOffset"
        offsetsService.write(s"${streamId}_offset", lastOffset + 1L)
      }
      if(offsetsPriority.length >0) {
        val lastOffset = offsetsPriority.map(_.offset).max
          logger debug s"committing last offset priority: $lastOffset"
        offsetsService.write(s"${streamId}.p_offset", lastOffset + 1L)
      }
    }
  }(Keep.right)


  val indexerGraph = RunnableGraph.fromGraph(GraphDSL.create(indexCommandsSource, priorityIndexCommandsSource, commitOffsets)((_,_,M) => M ) { implicit builder =>
    (batchSource, prioritySource, sink) =>
      import GraphDSL.Implicits._

      def newIndexCommandToEsAction(infoton:Infoton, isCurrent:Boolean, indexName:String):Try[InfoAction] = {
        Try {
          val indexTime = if(infoton.indexTime.isDefined) None else Some(System.currentTimeMillis())
          val infotonWithUpdatedIndexTime =
            if (indexTime.isDefined)
              infoton.replaceIndexTime(indexTime.get)
            else
              infoton
          val serializedInfoton = JsonSerializerForES.encodeInfoton(infotonWithUpdatedIndexTime, isCurrent)

          val indexRequest: ActionRequest[_ <: ActionRequest[_ <: AnyRef]] =
            Requests.indexRequest(indexName).`type`("infoclone").id(infoton.uuid).create(true).source(serializedInfoton)
            logger debug s"creating es actions for indexNewInfotonCommand: $indexRequest"
          InfoAction(indexRequest, infoton.weight, indexTime)
        }.recoverWith{
          case t:Throwable =>
            redlog info ("exception while encoding infoton to json for ES. will be ignored", t)
            Failure(t)
        }
      }

      val mergePrefferedSources = builder.add(
        MergePreferred[BGMessage[IndexCommand]](1, true)
      )

      val getInfotonIfNeeded = builder.add(
        Flow[BGMessage[IndexCommand]].mapAsync(math.max(numOfCassandraNodes/2, 2)){
          case bgMessage@BGMessage(_, inic@IndexNewInfotonCommand(uuid, _, _,None, _, _)) =>
            irwService.readUUIDAsync(uuid, QUORUM).map{
              case FullBox(infoton) => Success(bgMessage.copy(message = inic.copy(infotonOpt = Some(infoton)).asInstanceOf[IndexCommand]))
              case EmptyBox =>
                val e = new RuntimeException(s"Infoton for uuid: $uuid was not found by irwService. ignoring command")
                  redlog info ("", e)
                Failure(e)
              case BoxedFailure(t) =>
                val e = new RuntimeException(s"Exception from irwService while reading uuid: $uuid", t)
                  redlog info ("", e)
                Failure(e)
            }
          case bgMessage => Future.successful(Success(bgMessage))
        }.collect {
          case Success(x) => x
        }
      )

      val indexCommandToEsActions = builder.add(
        Flow[BGMessage[IndexCommand]].map{
          case bgMessage@BGMessage(_, inic@IndexNewInfotonCommand(uuid, isCurrent, path, Some(infoton), indexName, tids)) =>
            indexNewInfotonCommandCounter += 1
            newIndexCommandToEsAction(infoton, isCurrent, indexName).map{ ia => bgMessage.copy(message = (ia, inic.asInstanceOf[IndexCommand]))}
          case bgMessage@BGMessage(_, ieic@IndexExistingInfotonCommand(uuid, weight, _, indexName, tids)) =>
            logger debug s"creating es actions for indexExistingInfotonCommand: $ieic"
            indexExistingCommandCounter += 1
            val updateRequest = new UpdateRequest(indexName, "infoclone", uuid).version(1)
              .doc(s"""{"system":{"current": false}}""").asInstanceOf[ActionRequest[_ <: ActionRequest[_ <: AnyRef]]]
            Success(bgMessage.copy(message = (InfoAction(updateRequest, weight, None), ieic.asInstanceOf[IndexCommand])))
        }.collect{
          case Success(x) => x
        }
      )

      val groupEsActions = builder.add(
        Flow[BGMessage[(InfoAction, IndexCommand)]].groupedWeightedWithin(esActionsBulkSize, esActionsGroupingTtl.milliseconds)(_.message._1.weight)
      )

      val indexInfoActionsFlow = builder.add(
        Flow[Seq[BGMessage[(InfoAction, IndexCommand)]]].mapAsync(1){ bgMessages =>
          val esIndexRequests = bgMessages.map{
            case BGMessage(_, (InfoAction(esAction, _, indexTime), _)) => ESIndexRequest(esAction, indexTime)
          }
            logger debug s"${esIndexRequests.length} actions to index: \n${esIndexRequests.map{ ir => if (ir.esAction.isInstanceOf[UpdateRequest]) ir.esAction.asInstanceOf[UpdateRequest].doc().toString else ir.esAction.toString}}"
          val highestOffset = bgMessages.map(_.offsets).flatten.max
          indexBulkSizeHist += esIndexRequests.size
          indexingTimer.timeFuture(cmwell.util.concurrent.retry(10, 10.seconds, 1.15)(ftsService.executeBulkIndexRequests(esIndexRequests))).map{ bulkIndexResult =>
            BGMessage(highestOffset, (bulkIndexResult, bgMessages.map{_.message._2}))
          }
        }
      )

      val updateIndexInfoInCas = builder.add(
        Flow[BGMessage[(SuccessfulBulkIndexResult, Seq[IndexCommand])]].mapAsync(math.max(numOfCassandraNodes/3, 2)){
          case bgMessage@BGMessage(_, (bulkRes, indexCommands)) =>
              logger debug s"updating index time in cas for index commands: $indexCommands"
            val indexTimesToUpdate = bulkRes.successful.collect{case SuccessfulIndexResult(uuid, Some(indexTime)) =>
              (uuid, indexTime)
            }
              logger debug s"indexInfo to update: (uuid/indexTime/indexName): $indexTimesToUpdate"
            Future.sequence{
              indexTimesToUpdate.map{ case (uuid, indexTime) =>
                irwService.addIndexTimeToUuid(uuid, indexTime, QUORUM)
              }
            }.map{_ => bgMessage.copy(message = indexCommands)}
        }
      )

      val reportProcessTracking = builder.add(
        Flow[BGMessage[Seq[IndexCommand]]].mapAsync(3){
          case BGMessage(offsets, indexCommands) =>
              logger debug s"reporting process tracking for offsets: $offsets ,index commands: $indexCommands"
            Future.traverse(indexCommands){indexCommand =>
              TrackingUtilImpl.updateSeq(indexCommand.path, indexCommand.trackingIDs).recover{
                case t: Throwable =>
                  logger.error(s"updateSeq for path [${indexCommand.path}] with trackingIDs [${indexCommand.trackingIDs.mkString(",")}] failed",t)
              }
            }.map(_ => offsets)
        }
      )

      prioritySource ~> mergePrefferedSources.preferred
      batchSource ~> mergePrefferedSources.in(0)
      mergePrefferedSources ~> heartBitLog ~> getInfotonIfNeeded ~> indexCommandToEsActions ~> groupEsActions ~> indexInfoActionsFlow ~> updateIndexInfoInCas ~> reportProcessTracking ~> sink



    ClosedShape
  })

  val decider: Supervision.Decider = {

    case t:Throwable =>
      logger error ("Unexpected Exception during Indexer stream, sending 503 to BGActor and stopping stream", t)
      bgActor ! Indexer503
      Supervision.Stop
  }

  val indexerControl = indexerGraph.withAttributes(supervisionStrategy(decider)).run()

  indexerControl.onComplete{
    case Failure(t) =>
      logger error ("indexer stream stopped abnormally", t)
    case Success(x) =>
      logger info ("indexer stream stopped normally", x)
  }

  def shutdown = {
      logger warn "IndexerStream requested to shutdown"
    if(!indexerControl.isCompleted)
      sharedKillSwitch.shutdown()
    Try{Await.ready(indexerControl, 10.seconds)}.recover{ case t:Throwable =>
      logger error s"Indexer stream failed to shutdown after waiting for 10 seconds."
    }
      logger warn "IndexerStream is down"
  }

}
