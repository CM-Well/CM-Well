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

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.contrib.PartitionWith
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Partition, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape, Supervision}
import cmwell.bg.imp.{CommandsSource, RefsEnricher}
import cmwell.common._
import cmwell.common.formats.{BGMessage, JsonSerializerForES, Offset, PartialOffset}
import cmwell.domain.{Infoton, ObjectInfoton, SystemFields}
import cmwell.fts._
import cmwell.irw.IRWService
import cmwell.tracking._
import cmwell.util.concurrent.SimpleScheduler._
import cmwell.util.concurrent.travector
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.zstore.ZStore
import com.datastax.driver.core.ConsistencyLevel
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import nl.grons.metrics4.scala._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.{Iterable => IIterable}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Created by israel on 14/06/2016.
  */
class ImpStream(partition: Int,
                config: Config,
                irwService: IRWService,
                zStore: ZStore,
                ftsService: FTSService,
                offsetsService: OffsetsService,
                bgActor: ActorRef,
                bGMetrics: BGMetrics)(implicit actorSystem: ActorSystem,
                                      executionContext: ExecutionContext,
                                      materializer: ActorMaterializer)
  extends DefaultInstrumented {

  implicit val logger = Logger[ImpStream]

  lazy val redlog = LoggerFactory.getLogger("bg_red_log")
  lazy val heartbitLogger = LoggerFactory.getLogger("heartbeat_log")
  val parentsCache =
    CacheBuilder.newBuilder().maximumSize(4000 * 10).build[String, String]()
  val beforePersistedCache = new ConcurrentHashMap[String, Infoton]()

  val merger = Merger()

  val bootStrapServers = config.getString("cmwell.bg.kafka.bootstrap.servers")
  val persistCommandsTopic = config.getString("cmwell.bg.persist.commands.topic")
  val persistCommandsTopicPriority = persistCommandsTopic + ".priority"
  val indexCommandsTopic = config.getString("cmwell.bg.index.commands.topic")
  val indexCommandsTopicPriority = indexCommandsTopic + ".priority"
  val maxInfotonWeightToIncludeInCommand = config.getBytes("cmwell.bg.maxInfotonWeightToIncludeInCommand")
  val defaultDC = config.getString("cmwell.dataCenter.id")
  val esActionsBulkSize = config.getInt("cmwell.bg.esActionsBulkSize") // in bytes
  val esActionsGroupingTtl = config.getInt("cmwell.bg.esActionsGroupingTtl") // ttl for bulk es actions grouping in ms
  val groupCommandsByPathSize = config.getInt(
    "cmwell.bg.groupCommandsByPathSize"
  ) // # of commands to group by path
  val groupCommandsByPathTtl = config.getInt("cmwell.bg.groupCommandsByPathTtl") // timeout for the above grouping
  val maxDocsPerShard = config.getLong("cmwell.bg.maxDocsPerShard")
  val streamId = s"imp.${partition}"
  val startingOffset = offsetsService.read(s"${streamId}_offset").getOrElse(0L)
  val startingOffsetPriority = offsetsService.read(s"${streamId}.p_offset").getOrElse(0L)
  @volatile var (startingIndexName, startingIndexCount) =
    ftsService.latestIndexNameAndCount(s"cm_well_p${partition}_*") match {
      case Some((name, count)) => (name -> count)
      case None =>
        logger.info(
          s"no indexes found for partition $partition, creating first one"
        )
        Try {
          Await.result(
            ftsService.createIndex(s"cm_well_p${partition}_0").flatMap {
              createResponse =>
                if (createResponse.isAcknowledged) {
                  logger.info(
                    s"successfully created first index for partition $partition"
                  )
                  Future.successful(true)
                } else
                  Future.failed(
                    new RuntimeException(
                      s"failed to create first index for partition: $partition"
                    )
                  )
            },
            10.seconds
          )
        }.recover {
          case t: Throwable =>
            logger.error("failed to init ES index/alias, aborting !!!", t)
            throw t
        }

        (s"cm_well_p${partition}_0" -> 0L)
    }
  @volatile var currentIndexName = startingIndexName
  @volatile var fuseOn = config.getBoolean("cmwell.bg.fuseOn")
  @volatile var isRecoveryMode = true
  @volatile var isRecoveryModePriority = true

  private def isRecoveryModeForTopic(topic: String): Boolean =
    topic == persistCommandsTopic && isRecoveryMode || topic == persistCommandsTopicPriority && isRecoveryModePriority

  val (initialWriteHead, initialWriteHeadPriority) = {
    val tp1 = new TopicPartition(persistCommandsTopic, partition)
    val tp2 = new TopicPartition(persistCommandsTopicPriority, partition)
    val kafkaConsumerProps = new Properties()
    kafkaConsumerProps.put("bootstrap.servers", bootStrapServers)
    kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaConsumerProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaConsumerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaConsumerProps)
    val endOffsetsByTopicPartition = consumer.endOffsets(Seq(tp1,tp2).asJavaCollection)
    consumer.close()
    endOffsetsByTopicPartition.get(tp1) -> endOffsetsByTopicPartition.get(tp2)
  }

  logger.info(s"ImpStream($streamId), startingOffset: $startingOffset, startingOffsetPriority: $startingOffsetPriority; " +
    s"initalWriteHead (kafka.endOffset for $persistCommandsTopic($partition)): $initialWriteHead, " +
    s"initialWriteHeadPriority (kafka.endOffset for $persistCommandsTopicPriority($partition)): $initialWriteHeadPriority")

  val numOfShardPerIndex = ftsService.numOfShardsForIndex(currentIndexName)

  logger.info(s"ImpStream($streamId), startingIndexName: $startingIndexName, startingIndexCount: $startingIndexCount")
  val (cmdsSrcFromKafka, sharedKillSwitch) = CommandsSource.fromKafka(
    persistCommandsTopic,
    bootStrapServers,
    partition,
    startingOffset,
    startingOffsetPriority
  )
  val heartBitLog = Flow[BGMessage[Command]]
    .keepAlive(
      60.seconds,
      () => BGMessage(HeartbitCommand.asInstanceOf[Command])
  )
    .filterNot {
    case BGMessage(_, HeartbitCommand) =>
        heartbitLogger.info("Imp alive !!!")
      true
    case _ => false
  }
  val byteArraySerializer = new ByteArraySerializer()
  val kafkaProducerSettings =
    ProducerSettings(actorSystem, byteArraySerializer, byteArraySerializer)
      .withBootstrapServers(bootStrapServers)
      .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"lz4")
  val numOfCassandraNodes = config.getInt("cmwell.bg.num.of.cassandra.nodes")
  val irwReadConcurrency = Try {
    config.getInt("cmwell.bg.irwReadConcurrency")
  }.recover {
    case _: Throwable =>
    math.max(numOfCassandraNodes / 2, 2)
  }.get
  val irwWriteConcurrency = Try {
    config.getInt("cmwell.bg.irwWriteConcurrency")
  }.recover {
    case _: Throwable =>
    math.max(numOfCassandraNodes / 3, 2)
  }.get
  val groupCommandsByPath = Flow[BGMessage[SingleCommand]]
    .groupedWithin(groupCommandsByPathSize, groupCommandsByPathTtl.milliseconds)
    .mapConcat[BGMessage[(String, Seq[SingleCommand])]] { messages =>
      logger.debug(s"grouping commands: ${messages.map(_.message)}")

      val groupedByPath = messages.zipWithIndex.groupBy(_._1.message.path)

      groupedByPath.view.map {
        case (path, bgMessages) =>
          val offsets = bgMessages.flatMap {
            case (BGMessage(offsets, _), _) => offsets
          }
          val commands = bgMessages.map {
            case (BGMessage(_, messages), _) => messages
          }
          val minIndex = bgMessages.minBy { case (_, index) => index }._2
          (minIndex, BGMessage(offsets, path -> commands))
      }.to(Seq)
        .sortBy { case (index, _) => index }
        .map { case (_, bgMessage) => bgMessage }
    }
  val addLatestInfotons =
    Flow[BGMessage[(String, Seq[SingleCommand])]].mapAsync(irwReadConcurrency) {
      case bgMessage@BGMessage(_, (path, commands)) =>
        logger.debug(s"reading base infoton by path: $path")
    val start = System.currentTimeMillis()
    irwService.readPathAsync(path, ConsistencyLevel.QUORUM).map {
      case BoxedFailure(e) =>
        logger.error(s"readPathAsync failed for [$path]", e)
        bgMessage.copy(message = Option.empty[Infoton] -> commands)
      case box =>
            logger.debug(
              s"got base infoton for path: $path from irw: ${box.toOption}"
            )
        val end = System.currentTimeMillis()
        if (box.isDefined)
              bGMetrics.casFullReadTimer.update((end - start).millis)
        else
              bGMetrics.casEmptyReadTimer.update((end - start).millis)
        bgMessage.copy(message = box.toOption -> commands)
    }
  }
  val addMerged = Flow[BGMessage[(Option[Infoton], Seq[SingleCommand])]].map {
    case bgMessage@BGMessage(_, (existingInfotonOpt, commands)) =>
      beforePersistedCache.synchronized {
        val baseInfoton =
          Option(beforePersistedCache.get(commands.head.path)) match {
          case None =>
              logger.debug(
                s"baseInfoton for path: ${commands.head.path} not in cache"
              )
            existingInfotonOpt
          case cachedInfotonOpt@Some(cachedInfoton) =>
              logger.debug(
                s"base infoton for path: ${commands.head.path} in cache: $cachedInfoton"
              )
            existingInfotonOpt match {
              case None => cachedInfotonOpt
              case Some(existingInfoton) =>
                if (cachedInfoton.systemFields.lastModified.getMillis < existingInfoton.systemFields.lastModified.getMillis) {
                    logger.debug(s"cached infoton is newer then base infoton")
                  cachedInfotonOpt.map { i =>
                    i.copyInfoton(i.systemFields.copy(lastModified = existingInfoton.systemFields.lastModified))
                  }
                  } else {
                    logger.debug("base infoton is newer then cached infoton")
                  cachedInfotonOpt
                }
            }
        }
        logger.debug(
          s"merging existing infoton: $baseInfoton with commands: $commands"
        )

        val mergedInfoton =
          if (baseInfoton.isDefined || commands.size > 1)
            bGMetrics.mergeTimer.time(merger.merge(baseInfoton, commands))
          else
            merger.merge(baseInfoton, commands)
        mergedInfoton.merged.foreach { i =>
          beforePersistedCache.put(
            i.systemFields.path,
            i.copyInfoton(i.systemFields.copy(indexName = currentIndexName))
          )
          schedule(60.seconds) {
            beforePersistedCache.remove(i.systemFields.path)
          }
        }
        bgMessage.copy(message = (baseInfoton -> mergedInfoton))
      }
  }

  val persistNewLastModified = Flow[BGMessage[(Option[Infoton],MergeResponse)]].mapAsync(irwWriteConcurrency) {
    case bm@BGMessage(offsets, (_, mr)) => mr.extra.fold(Future.successful(bm)) { extra =>
      //TODO propagate offsets.retention.minutes's value to here
      val ttlSeconds = 7.days.toSeconds.toInt
      val offset = offsets.last
      val key = s"imp.$partition${if (offset.topic == persistCommandsTopicPriority) ".p" else ""}_${offset.offset}"
      val payload = extra.getBytes(StandardCharsets.UTF_8)
      zStore.put(key, payload, ttlSeconds, batched = true).
        recover { case t => logger.error(s"Could not write to zStore ($key,$extra)", t) }.
        map(_ => bm)
    }
  }

  val filterDups = Flow[BGMessage[(Option[Infoton], Infoton)]].filterNot {
    bgMessage =>
      val isDup = bgMessage.message._1
        .map {
      _.uuid
        }
        .getOrElse("")
        .equals(bgMessage.message._2.uuid)
      logger.debug(s"filtering infoton due to same uuid: ${
        bgMessage.message._1
          .map(_.uuid)
          .getOrElse("")
      } equals ${bgMessage.message._2.uuid}")
    isDup
  }
  val publishVirtualParentsSink =
    Producer.plainSink[Array[Byte], Array[Byte]](kafkaProducerSettings)
  val createVirtualParents =
    Flow[BGMessage[Seq[(IndexCommand, Option[DateTime])]]]
      .mapAsync(1) {
    case BGMessage(offsets, indexCommands) =>
          logger.debug(
            s"checking missing parents for index commands: $indexCommands"
          )

          val parentsWithChildDate =
            cmwell.util.collections.distinctBy(indexCommands.collect {
              case (indexCommand, Some(lastModified)) =>
                Infoton.getParent(indexCommand.path) -> lastModified
            })(_._1)

          Future
            .traverse(parentsWithChildDate)(checkParent)
            .map(_.view.collect {
              case (true, (path, childLastModified)) =>
                val infoton = ObjectInfoton(SystemFields(path, childLastModified, "AUTO CREATED", defaultDC, None, currentIndexName, "http"))
                val writeCommand = WriteCommand(infoton)
                val payload = CommandSerializer.encode(writeCommand)
                val topic =
                  if(offsets.exists(_.topic.endsWith("priority")))
                    persistCommandsTopicPriority
                  else
                    persistCommandsTopic
                        new ProducerRecord[Array[Byte], Array[Byte]](
                          topic,
                          infoton.systemFields.path.getBytes,
                          payload
              )
            }.to(IIterable))
      }
      .mapConcat(identity)

  val extractIndexCommands = Flow[BGMessage[(Option[Infoton], (Infoton, Seq[String]))]]
    .mapAsync(irwWriteConcurrency) {
    case BGMessage(offsets, (previous, (latest, trackingIds))) =>
      val latestWithIndexName = latest.copyInfoton(latest.systemFields.copy(indexName = currentIndexName))
        logger.debug(s"writing lastest infoton: $latestWithIndexName")
        val isRecoveryRequired = offsets.exists(o => isRecoveryModeForTopic(o.topic))
        irwService
          .writeAsync(latestWithIndexName, ConsistencyLevel.QUORUM)
          .flatMap { i =>
            val ini: (IndexCommand, Option[DateTime]) = IndexNewInfotonCommand(i.uuid, true, i.systemFields.path, Some(i), i.systemFields.indexName,
              trackingIds.map(StatusTracking(_, 1))) -> Some(i.systemFields.lastModified)
            if (previous.isEmpty) {
              Future.successful(List(BGMessage(offsets, Seq(ini))))
            } else if(previous.get.isSameAs(latest)) {
              if(!isRecoveryRequired) {
                logger.warn(s"Previous [${previous.get.uuid}] isSameAs Latest but recoveryMode is off, this should only happen to parent Infotons.")
                Future.successful(List(BGMessage(offsets, Seq(ini))))
              } else {
        // this case is when we're replaying persist command which was not indexed at all (due to error of some kind)
                val previousTimestamp = previous.get.systemFields.lastModified.getMillis
                irwService.historyNeighbourhood(previous.get.systemFields.path, previousTimestamp,
                  desc = true, limit = 2, ConsistencyLevel.QUORUM).flatMap { previousAndOneBeforeThat =>
                  val statusTracking = trackingIds.map(StatusTracking(_, 1))
                  val indexNewInfoton: (IndexCommand, Option[DateTime]) =
                    IndexNewInfotonCommand(i.uuid, true, i.systemFields.path, Some(i), i.systemFields.indexName, statusTracking) ->
                      Some(i.systemFields.lastModified)
                  val oldUuidOpt = previousAndOneBeforeThat.find(_._1 != previous.get.systemFields.lastModified.getMillis).map(_._2)
                  oldUuidOpt.fold{
                    Future.successful(List(BGMessage(offsets, Seq(indexNewInfoton))))
                  }{ oldUuid =>
                    irwService.readUUIDAsync(oldUuid, ConsistencyLevel.QUORUM, dontFetchPayload = true).map {
                      case FullBox(oldInfoton) =>
                        val statusTracking = trackingIds.map(StatusTracking(_, 2))
                        val indexNewInfoton: (IndexCommand, Option[DateTime]) =
                          IndexNewInfotonCommand(i.uuid, true, i.systemFields.path, Some(i), i.systemFields.indexName, statusTracking) ->
                            Some(i.systemFields.lastModified)
                        val indexExistingInfoton: (IndexCommand, Option[DateTime]) =
                          IndexExistingInfotonCommand(oldInfoton.uuid, oldInfoton.weight, oldInfoton.systemFields.path, oldInfoton.systemFields.indexName,
                            statusTracking) -> None
                        val updatedOffsetsForNew = offsets.map { o =>
                          PartialOffset(o.topic, o.offset, 1, 2)
                        }
                        val updatedOffsetsForExisting = offsets.map { o =>
                          PartialOffset(o.topic, o.offset, 2, 2)
          }
                        List(
                          BGMessage(updatedOffsetsForExisting, Seq(indexExistingInfoton)),
                          BGMessage(updatedOffsetsForNew, Seq(indexNewInfoton))
                        )
                      case EmptyBox =>
                        redlog.error(s"UUID $oldUuid was in the neighbourhood when merging path ${previous.get.systemFields.path} - " +
                          s"but is not in infoton table - this might introduce a Duplicate!")
                        List(BGMessage(offsets, Seq(indexNewInfoton)))
                      case BoxedFailure(t) =>
                        redlog.error(s"UUID $oldUuid was in the neighbourhood when merging path ${previous.get.systemFields.path} - " +
                          s"but could not be read from infoton table (see exception) - this might introduce a Duplicate!", t)
          List(BGMessage(offsets, Seq(indexNewInfoton)))
                    }
                  }
                }
              }
        } else {
              val statusTracking = trackingIds.map(StatusTracking(_, 2))
              val indexNewInfoton: (IndexCommand, Option[DateTime]) = IndexNewInfotonCommand(i.uuid, true, i.systemFields.path, Some(i),
                i.systemFields.indexName, statusTracking) -> Some(i.systemFields.lastModified)
              val indexExistingInfoton: (IndexCommand, Option[DateTime]) =
                IndexExistingInfotonCommand(previous.get.uuid, previous.get.weight, previous.get.systemFields.path, previous.get.systemFields.indexName,
                  statusTracking) -> None
              val updatedOffsetsForNew = offsets.map { o =>
                PartialOffset(o.topic, o.offset, 1, 2)
              }
              val updatedOffsetsForExisting = offsets.map { o =>
                PartialOffset(o.topic, o.offset, 2, 2)
              }
              Future.successful(List(
                BGMessage(updatedOffsetsForExisting, Seq(indexExistingInfoton)),
                BGMessage(updatedOffsetsForNew, Seq(indexNewInfoton))
              ))
          }
        }
      }
    .mapConcat(identity)
  val mergedCommandToKafkaRecord =
    Flow[BGMessage[(BulkIndexResult, Seq[IndexCommand])]].mapConcat {
      case BGMessage(offsets, (bulkIndexResults, commands)) =>
        logger.debug(
          s"failed indexcommands to kafka records. bulkResults: $bulkIndexResults \n commands: $commands"
        )
        val failedCommands =
          if (bulkIndexResults.isInstanceOf[RejectedBulkIndexResult])
        commands
      else {
        val failedUUIDs = bulkIndexResults.failed.map {
          _.uuid
        }.toSet
            commands.filter { cmd =>
              failedUUIDs.contains(cmd.uuid)
            }
      }

      failedCommands.view.map { failedCommand =>
          val commandToSerialize =
            (if (failedCommand.isInstanceOf[IndexNewInfotonCommand] &&
              failedCommand
                .asInstanceOf[IndexNewInfotonCommand]
                .infotonOpt
                .get
                .weight > maxInfotonWeightToIncludeInCommand) {
              logger.debug(
                s"infoton weight: ${failedCommand.asInstanceOf[IndexNewInfotonCommand].infotonOpt.get.weight} of failed command exceeds max weight"
              )
              failedCommand
                .asInstanceOf[IndexNewInfotonCommand]
                .copy(infotonOpt = None)
        } else {
          failedCommand
            }) match {
              case cmd: IndexNewInfotonCommand =>
                IndexNewInfotonCommandForIndexer(cmd.uuid, cmd.isCurrent, cmd.path, cmd.infotonOpt, cmd.indexName, offsets, cmd.trackingIDs)
              case cmd: IndexExistingInfotonCommand =>
                IndexExistingInfotonCommandForIndexer(cmd.uuid, cmd.weight, cmd.path, cmd.indexName, offsets, cmd.trackingIDs)
              case x @ (IndexExistingInfotonCommandForIndexer(_, _, _, _, _,_) | IndexNewInfotonCommandForIndexer(_, _, _, _, _,_,_) |
                   NullUpdateCommandForIndexer(_, _, _, _,_)) => logger.error(s"Unexpected input. Received: $x"); ???
        }
        val topic =
            if (offsets.exists(_.topic.endsWith("priority")))
            indexCommandsTopicPriority
          else
            indexCommandsTopic
        Message(
            new ProducerRecord[Array[Byte], Array[Byte]](
              topic,
              commandToSerialize.path.getBytes,
              CommandSerializer.encode(commandToSerialize)
            ),
            offsets
        )
        }.to(Seq)
  }
  val indexCommandsToKafkaRecords =
    Flow[BGMessage[Seq[IndexCommand]]].mapConcat {
      case BGMessage(offsets, commands) =>
        logger.debug(s"converting index commands to kafka records:\n$commands")
      commands.view.map { command =>
        val commandToSerialize =
            (if (command.isInstanceOf[IndexNewInfotonCommand] &&
              command
                .asInstanceOf[IndexNewInfotonCommand]
                .infotonOpt
                .get
                .weight > maxInfotonWeightToIncludeInCommand) {
              command
                .asInstanceOf[IndexNewInfotonCommand]
                .copy(infotonOpt = None)
          } else
              command) match {
              case cmd: IndexNewInfotonCommand =>
                IndexNewInfotonCommandForIndexer(cmd.uuid, cmd.isCurrent, cmd.path, cmd.infotonOpt, cmd.indexName, offsets, cmd.trackingIDs)
              case cmd: IndexExistingInfotonCommand =>
                IndexExistingInfotonCommandForIndexer(cmd.uuid, cmd.weight, cmd.path, cmd.indexName, offsets, cmd.trackingIDs)
              case x @ (IndexExistingInfotonCommandForIndexer(_, _, _, _, _, _) | IndexNewInfotonCommandForIndexer(_, _, _, _, _, _, _) |
                   NullUpdateCommandForIndexer(_, _, _, _, _)) => logger.error(s"Unexpected input. Received: $x"); ???
            }
          val topic =
            if (offsets.exists(_.topic.endsWith("priority")))
              indexCommandsTopicPriority
            else
              indexCommandsTopic
          Message(
            new ProducerRecord[Array[Byte], Array[Byte]](
              topic,
              commandToSerialize.path.getBytes,
              CommandSerializer.encode(commandToSerialize)
            ),
            offsets
          )
        }.to(Seq)
    }
  val nullCommandsToKafkaRecords: Flow[BGMessage[(Option[Infoton], MergeResponse)], Message[Array[Byte], Array[Byte], Seq[Offset]], NotUsed] =
    Flow.fromFunction {
      case msg@BGMessage(offsets, (_, NullUpdate(path, tids, evictions, _))) =>
        logger.debug(s"converting null command to kafka records:\n path: $path, offsets: [$offsets]")
        val nullCmd = NullUpdateCommandForIndexer(path = path, persistOffsets = offsets)
        val topic =
          if (offsets.exists(_.topic.endsWith("priority")))
            indexCommandsTopicPriority
          else
            indexCommandsTopic
        Message(
          new ProducerRecord[Array[Byte], Array[Byte]](
            topic,
            nullCmd.path.getBytes,
            CommandSerializer.encode(nullCmd)
          ),
          offsets
        )
  }
  val publishIndexCommandsFlow =
    Flow[Message[Array[Byte], Array[Byte], Seq[Offset]]]
      .via(
      Producer
        .flexiFlow[Array[Byte], Array[Byte], Seq[Offset]](kafkaProducerSettings)
        .map { _.passThrough }
        )

  // @formatter:off
  val impGraph = RunnableGraph.fromGraph(
    GraphDSL.create(cmdsSrcFromKafka,
      OffsetUtils.commitOffsetSink(streamId, persistCommandsTopic, startingOffset,startingOffsetPriority, offsetsService),
      publishVirtualParentsSink)((_, a, b) => (a, b)) { implicit builder =>
      (commandsSource, commitOffsets, publishVirtualParents) =>
      import GraphDSL.Implicits._

      // partition incoming persist commands. override commands goes to outport 0, all the rest goes to 1
      val singleCommandsPartitioner = builder.add(
        Partition[BGMessage[SingleCommand]](
          2, {
            case BGMessage(_, singleCommand) =>
              if (singleCommand.isInstanceOf[OverwriteCommand])
                0
              else
                1
          }
        )
      )

        val checkRecoveryState = builder.add(
          Flow[BGMessage[Command]].map { cmd =>
            val (nonPriorityOffsets, priorityOffsets) = cmd.offsets.partition(_.topic == persistCommandsTopic)
            if (isRecoveryMode && nonPriorityOffsets.forall(_.offset >= initialWriteHead)) {
              logger.info(s"Quitting Recovery Mode for $persistCommandsTopic($partition)")
              isRecoveryMode = false
            }
            if (isRecoveryModePriority && priorityOffsets.forall(_.offset >= initialWriteHeadPriority)) {
              logger.info(s"Quitting Recovery Mode for $persistCommandsTopicPriority($partition)")
              isRecoveryModePriority = false
            }
            cmd
          }
        )

      val partitionMerged = builder.add(
        Partition[BGMessage[(Option[Infoton], MergeResponse)]](2, {
          case BGMessage(_, (_, mergeResponse)) if mergeResponse.isInstanceOf[RealUpdate] => 0
          case _ => 1
        })
      )

      val reportNullUpdates = builder.add(
        Flow[BGMessage[(Option[Infoton], MergeResponse)]].mapAsync(1) {
            case msg@BGMessage(offsets, (_, NullUpdate(path, tids, evictions, _))) =>
              bGMetrics.nullUpdateCounter += 1
            val nullUpdatesReportedAsDone = {
              if (tids.nonEmpty)
                TrackingUtilImpl.updateSeq(path, tids.map(StatusTracking(_, 1))).map {
                  case LogicalFailure(reason) =>
                      logger.warn(s"Failed to report tracking due to: $reason")
                    Future.successful(())
                  case _ => Future.successful(())
                }
              else
                Future.successful(())
            }
            val evictionsReported = {
              Future.traverse(evictions) {
                case (reason, Some(tid)) => TrackingUtilImpl.updateEvicted(path, tid, reason).map {
                  case LogicalFailure(reason) =>
                      logger.warn(s"Failed to report eviction tracking due to: $reason")
                    Future.successful(())
                  case _ => Future.successful(())
                }
                  case (reason, None) => Future(logger.info(s"evicted command in path:$path because of: $reason"))
                }
              }
              val zStored = {
                val ttlSeconds = 7.days.toSeconds.toInt //TODO propagate offsets.retention.minutes's value to here
                travector(offsets) { o =>
                  val key = s"imp.$partition${if(o.topic == persistCommandsTopicPriority) ".p" else ""}_${o.offset}"
                  val payload = "nu".getBytes(StandardCharsets.UTF_8)
                  zStore.put(key, payload, ttlSeconds, batched = true).
                    recover { case t => logger.error(s"Could not write to zStore ($key,nu)", t) }
              }
            }

              val msgFuture = for {
              _ <- nullUpdatesReportedAsDone
              _ <- evictionsReported
                _ <- zStored
              } yield msg

              msgFuture
        }
      )

      val partitionNonNullMerged = builder.add(
        PartitionWith[BGMessage[(Option[Infoton], MergeResponse)],
          BGMessage[(Option[Infoton], (Infoton, Seq[String]))],
          (String, Seq[(String, Option[String])])] {
            case BGMessage(offset, (prev, RealUpdate(cur, tids, evicted, _))) =>
            if (evicted.nonEmpty)
              Right(cur.systemFields.path -> evicted)
            else
              Left(BGMessage(offset, (prev, (cur, tids))))
        }
      )

      val overrideCommandsToInfotons = builder.add(
        Flow[BGMessage[(String, Seq[SingleCommand])]].map{
          case bgMessage@BGMessage(_, (path, commands)) =>
              bgMessage.copy(message = (path, commands.map {
                case OverwriteCommand(infoton, trackingID) => (infoton, trackingID)
                case x @ (DeleteAttributesCommand(_, _, _, _, _, _) | DeletePathCommand(_, _, _, _, _) |
                          UpdatePathCommand(_, _, _, _, _, _, _,_) | WriteCommand(_, _, _))
                  => logger.error(s"Unexpected input. Received: $x"); ???
              }))
        }
      )

      val partitionOverrideInfotons = builder.add(
        PartitionWith[BGMessage[
          (String, Seq[(Infoton, Option[String])])],
          BGMessage[(String, Seq[(Infoton, Option[String])])],
          Seq[Offset]] {
          case bgMessage@BGMessage(offset, (_, infotons)) =>
            if (infotons.size > 0)
              Left(bgMessage)
            else
              Right(offset)
        }
      )

      val processOverrideInfotons = builder.add(
        Flow[BGMessage[(String, Seq[(Infoton, Option[String])])]].mapAsync(irwReadConcurrency) {
          case bgMessage@BGMessage(_, (path, infotons)) =>
            val sortedInfotons = infotons.sortWith { (l, r) =>
              if (l._1.systemFields.lastModified.getMillis != r._1.systemFields.lastModified.getMillis)
                l._1.systemFields.lastModified.getMillis > r._1.systemFields.lastModified.getMillis
              else
                l._1.uuid < r._1.uuid
            }
            irwService.readPathAsync(path, ConsistencyLevel.QUORUM).map {
              case BoxedFailure(e) =>
                logger.error(s"readPathAsync failed for [$path]", e)
                bgMessage.copy(message = (sortedInfotons, None))
              case b =>
                val infoOpt = b.toOption
                  logger.debug(s"readPathAsync for path:$path returned: ${b.toOption}")
                bgMessage.copy(message = (sortedInfotons, infoOpt))
            }
        }.mapAsync(irwWriteConcurrency) {
          case BGMessage(offsets, (newInfotons, existingInfotonOpt)) =>
            val newInfotonsWithIndexName = newInfotons.map { case (i, tid) => (i.copyInfoton(i.systemFields.copy(indexName = currentIndexName)) -> tid) }
            irwService.writeSeqAsync(newInfotonsWithIndexName.map {
              _._1
            }, ConsistencyLevel.QUORUM).map { writtenInfotons =>
              val bgMessages: List[BGMessage[Seq[IndexCommand]]] = newInfotonsWithIndexName.toList match {
                case ((headInfoton, headTrackingId) :: tail) =>
                  val isHeadCurrent = existingInfotonOpt.isEmpty || {
                    if (headInfoton.systemFields.lastModified.getMillis != existingInfotonOpt.get.systemFields.lastModified.getMillis)
                      headInfoton.systemFields.lastModified.getMillis > existingInfotonOpt.get.systemFields.lastModified.getMillis
                    else
                      headInfoton.uuid <= existingInfotonOpt.get.uuid
                  }
                  val numOfParts = if (existingInfotonOpt.isDefined) 2 else 1
                  val indexNewInfotonCommands: Seq[IndexCommand] = IndexNewInfotonCommand(headInfoton.uuid,
                    isHeadCurrent, headInfoton.systemFields.path, Some(headInfoton), headInfoton.systemFields.indexName,
                    Seq(headTrackingId).flatten.map {
                      StatusTracking(_, numOfParts)
                    }) :: tail.map { case (i, t) =>
                    IndexNewInfotonCommand(i.uuid, false, i.systemFields.path, Some(i), i.systemFields.indexName,
                      Seq(t).flatten.map {
                        StatusTracking(_, numOfParts)
                      })
                  }
                  existingInfotonOpt.fold{
                    List(BGMessage(offsets, indexNewInfotonCommands))
                  } { existingInfoton =>
                    val statusTracking = newInfotonsWithIndexName.map {
                      _._2
                    }.flatten.map {
                      StatusTracking(_, 2)
                    }

                    if (isHeadCurrent && !existingInfotonOpt.exists(_.uuid == headInfoton.uuid)) {
                      val updatedOffsetsForNew = offsets.map{ o => PartialOffset(o.topic, o.offset, 1, 2) }
                      val updatedOffsetsForExisting = offsets.map{ o => PartialOffset(o.topic, o.offset, 2, 2) }
                      val indexNew = BGMessage(updatedOffsetsForNew, indexNewInfotonCommands)
                      val indexExisting:BGMessage[Seq[IndexCommand]] = BGMessage(
                        updatedOffsetsForExisting,
                        Seq(IndexExistingInfotonCommand(existingInfoton.uuid, existingInfoton.weight,
                              existingInfoton.systemFields.path, existingInfoton.systemFields.indexName, statusTracking))
                      )
                      List(indexExisting, indexNew)
                    } else {
                      List(BGMessage(offsets, indexNewInfotonCommands))
                    }
                  }
                case _ => ???
              }

                logger.debug(s"override flow produced IndexCommands:\n$bgMessages")

              bgMessages
            }
        }.mapConcat(identity)
      )

      val groupEsActions = builder.add(
        Flow[BGMessage[(Seq[(ESIndexRequest, Long)], Seq[IndexCommand])]].groupedWeightedWithin(
          esActionsBulkSize, esActionsGroupingTtl.milliseconds)(_.message._1.map(_._2).foldLeft(0L)(_ + _))
      )

      // temp fix
      val filterNonEmpty = builder.add(Flow[Seq[BGMessage[(Seq[(ESIndexRequest, Long)], Seq[IndexCommand])]]].filter {
        _.nonEmpty
      })

      val indexCommandsToESActions = builder.add(
        Flow[BGMessage[Seq[IndexCommand]]].map { case bgMessage@BGMessage(_, indexCommands) =>
          val actionRequests = indexCommands.flatMap {
            case IndexNewInfotonCommand(_, isCurrent, _, Some(infoton), indexName, trackingIDs) =>
              // count it for metrics
                bGMetrics.indexNewInfotonCommandCounter += 1
              List(
                Try {
                  val indexTime = if (infoton.systemFields.indexTime.isDefined) None else Some(System.currentTimeMillis())
                  val infotonWithUpdatedIndexTime =
                    if (indexTime.isDefined)
                      infoton.replaceIndexTime(indexTime)
                    else
                      infoton
                  val serializedInfoton = JsonSerializerForES.encodeInfoton(infotonWithUpdatedIndexTime, isCurrent)
                  (ESIndexRequest(
                    Requests.indexRequest(indexName).id(infoton.uuid).create(true)
                      .source(serializedInfoton, XContentType.JSON),
                    indexTime
                  ), infotonWithUpdatedIndexTime.weight)
                }
              ).filter {
                case Success(_) => true
                case Failure(t) =>
                    redlog.info("failed to encode indexnewInfotonCommand's infoton, skipping it", t)
                  false
              }.map {
                _.get
              }
            case IndexExistingInfotonCommand(uuid, weight, _, indexName, _) =>
              //count it for metrics
                bGMetrics.indexExistingCommandCounter += 1
              List((ESIndexRequest(
                  new UpdateRequest(indexName, uuid).doc(s"""{"system":{"current": false}}""", XContentType.JSON),
                None
              ), weight))
            case _ => ???
          }
            logger.debug(s"creating esactions:\n $actionRequests from index commands:\n $indexCommands")
          bgMessage.copy(message = (actionRequests, indexCommands))
        }.filter {
          _.message._1.nonEmpty
        }
      )
      val sendActionsToES = builder.add(
        Flow[Seq[BGMessage[(Seq[(ESIndexRequest, Long)], Seq[IndexCommand])]]].mapAsync(1) {
          case bgMessages =>
            val actions = bgMessages.map {
              _.message._1.map {
                _._1
              }
            }.flatten
              logger.debug(s"sending ${actions.size} actions to elasticsearch: ${actions} from commands: ${
              bgMessages.map {
                _.message._2
              }.flatten
              }")
              bGMetrics.indexBulkSizeHist += actions.size
              bGMetrics.indexingTimer.timeFuture(ftsService.executeIndexRequests(actions).recover {
              case _: TimeoutException =>
                RejectedBulkIndexResult("Future Timedout")
            }
            ).map { bulkIndexResult =>
              // filter out expected es failures (due to we're at least once and not exactly once)
              val filteredResults = bulkIndexResult match {
                case s@SuccessfulBulkIndexResult(_, failed) if failed.size > 0 =>
                  s.copy(failed = failed.filterNot { f =>
                    (f.reason.startsWith("DocumentAlreadyExistsException") || f.reason.startsWith("VersionConflictEngineException"))
                  })
                case other => other
              }
              BGMessage(bgMessages.map(_.offsets).flatten, (filteredResults, bgMessages.map {
                _.message._2
              }.flatten))
            }
        }
      )

      val manageIndices = builder.add(
        Flow[BGMessage[(BulkIndexResult, Seq[IndexCommand])]].mapAsync(1) {
          case bgMessage@BGMessage(_, (bulkRes, commands)) =>
            startingIndexCount += bulkRes.successful.size
            if (startingIndexCount / numOfShardPerIndex >= maxDocsPerShard) {
              val (pref, suf) = currentIndexName.splitAt(currentIndexName.lastIndexOf('_') + 1)
              val nextCount = suf.toInt + 1
              val nextIndexName = pref + nextCount
              cmwell.util.concurrent.retry(3)(ftsService.createIndex(nextIndexName)).map { case _ =>
                currentIndexName = nextIndexName
                startingIndexCount = 0
                bgMessage
              }
            } else
              Future.successful(bgMessage)
        }
      )

      val updateIndexInfoInCas = builder.add(
        Flow[BGMessage[(BulkIndexResult, Seq[IndexCommand])]].mapAsync(math.max(numOfCassandraNodes / 3, 2)) {
          case bgMessage@BGMessage(_, (bulkRes, commands)) =>
              logger.debug(s"updating index info in cas for bulkRes: $bulkRes from commands: $commands")
            val indexTimeToUpdate = bulkRes.successful.collect { case SuccessfulIndexResult(uuid, Some(indexTime)) =>
              (uuid, indexTime)
            }

            Future.sequence {
              indexTimeToUpdate.map { case (uuid, indexTime) =>
                irwService.addIndexTimeToUuid(uuid, indexTime, ConsistencyLevel.QUORUM)
              }
            }.map { _ => bgMessage }
        }
      )

      val partitionIndexResult = builder.add(
        Partition[BGMessage[(BulkIndexResult, Seq[IndexCommand])]](2, {
          case BGMessage(_, (bulkIndexResult, commands)) =>
              logger.debug(s"Partitioning index results: $bulkIndexResult from commands: $commands")
            if (bulkIndexResult.isInstanceOf[RejectedBulkIndexResult]) {
                logger.info("ftservice rejected, turning fusing off and scheduling back on in 3 minutes")
              fuseOn = false
              cmwell.util.concurrent.SimpleScheduler.schedule(3.minutes) {
                  logger.info("turning fusing on after 3 minutes wait")
                fuseOn = true
              }
            }
            if (bulkIndexResult.isInstanceOf[RejectedBulkIndexResult] || bulkIndexResult.failed.size > 0) {
                logger.debug(s"BulkIndexResult:${bulkIndexResult.isInstanceOf[RejectedBulkIndexResult]} failed size:${bulkIndexResult.failed.size}")
              1
            } else {
                logger.debug("no failures in BulkIndexResult")
              0
            }
        })
      )

      val fusePartition = builder.add(
        Partition[BGMessage[Seq[IndexCommand]]](2, {
            //The mechanism that persists the offsets of the imp in the indexer is based on the fact that there will be no fusing at all - disabled in code!!!
            case bgMessage => 1 //if (fuseOn) 0 else 1
        })
      )

      val reportProcessTracking = builder.add(
        Flow[BGMessage[(BulkIndexResult, Seq[IndexCommand])]].mapAsync(3) {
          case BGMessage(offset, (_, indexCommands)) =>
              logger.debug(s"report process tracking of indexCommands: $indexCommands")
            Future.traverse(indexCommands) { indexCommand =>
              TrackingUtilImpl.updateSeq(indexCommand.path, indexCommand.trackingIDs).map {
                case LogicalFailure(reason) =>
                    val p = indexCommand.path
                    val tids = indexCommand.trackingIDs.mkString(",")
                    logger.warn(s"Failed to report tracking for path: $p with tids: $tids due to: $reason")
                  Future.successful(())
                case _ => Future.successful(())
              }
            }.map(_ => offset)
        }
      )

      val logOrReportEvicted = builder.add(
        Flow[(String, Seq[(String, Option[String])])].mapAsync(3) {
          case (path, evictions) =>
            Future.traverse(evictions) {
              case (reason, Some(tid)) => TrackingUtilImpl.updateEvicted(path, tid, reason).map {
                case LogicalFailure(reason) =>
                    logger.warn(s"Failed to report eviction tracking due to: $reason")
                  Future.successful(())
                case _ => Future.successful(())
              }
                case (reason, None) => Future(logger.info(s"evicted command in path:$path because of: $reason"))
            }
        }
      )

      val mergeCompletedOffsets = builder.add(Merge[Seq[Offset]](4))

      val indexCommandsMerge = builder.add(Merge[BGMessage[Seq[IndexCommand]]](2))

      val mergeKafkaRecords = builder.add(Merge[Message[Array[Byte], Array[Byte], Seq[Offset]]](2))

      val nonOverrideIndexCommandsBroadcast = builder.add(Broadcast[BGMessage[Seq[(IndexCommand, Option[DateTime])]]](2))

      val removeLastModified = builder.add(
        Flow[BGMessage[Seq[(IndexCommand, Option[DateTime])]]].map { msg =>
          msg.copy(message = msg.message.map(_._1))
        })

        val refsEnricher = RefsEnricher.toSingle(bGMetrics, irwReadConcurrency, zStore)

        commandsSource ~> heartBitLog ~> checkRecoveryState ~> refsEnricher ~> singleCommandsPartitioner

      singleCommandsPartitioner.out(0) ~> groupCommandsByPath ~> overrideCommandsToInfotons ~> partitionOverrideInfotons.in

      partitionOverrideInfotons.out0 ~> processOverrideInfotons ~> indexCommandsMerge.in(0)

      partitionOverrideInfotons.out1 ~> mergeCompletedOffsets.in(0)

        singleCommandsPartitioner.out(1) ~> groupCommandsByPath ~> addLatestInfotons ~> addMerged ~> persistNewLastModified ~> partitionMerged.in

      partitionMerged.out(0) ~> partitionNonNullMerged.in

        partitionNonNullMerged.out0 ~> extractIndexCommands ~> nonOverrideIndexCommandsBroadcast.in

      partitionNonNullMerged.out1 ~> logOrReportEvicted ~> Sink.ignore

        partitionMerged.out(1) ~> reportNullUpdates ~> nullCommandsToKafkaRecords ~> publishIndexCommandsFlow ~> mergeCompletedOffsets.in(1)

      nonOverrideIndexCommandsBroadcast.out(0) ~> createVirtualParents ~> publishVirtualParents

      nonOverrideIndexCommandsBroadcast.out(1) ~> removeLastModified ~> indexCommandsMerge.in(1)

      indexCommandsMerge.out ~> fusePartition

        //Note: below stream is dead code!
        fusePartition.out(0) ~> indexCommandsToESActions ~> groupEsActions ~> filterNonEmpty ~>
          sendActionsToES ~> manageIndices ~> updateIndexInfoInCas ~> partitionIndexResult.in

      fusePartition.out(1) ~> indexCommandsToKafkaRecords ~> mergeKafkaRecords.in(0)

      // In case of partitial success of indexing, failed goes to index_topic and successful offset are not reported
      // Since we have one offset (lowest) per group of commands. When the failed will succeed in Indexer it will be
      // reported
      partitionIndexResult.out(0) ~> reportProcessTracking ~> mergeCompletedOffsets.in(2)

      partitionIndexResult.out(1) ~> mergedCommandToKafkaRecord ~> mergeKafkaRecords.in(1)

      mergeKafkaRecords.out ~> publishIndexCommandsFlow ~> mergeCompletedOffsets.in(3)

      mergeCompletedOffsets.out ~> commitOffsets

      ClosedShape
  }
  )
  val decider: Supervision.Decider = {

    case t:Throwable =>
      logger.error(
        "Unexpected Exception during Imp stream, sending 503 to BGActor and stopping stream",
        t
      )
      bgActor ! Imp503
      Supervision.Stop
  }
  val impControl = impGraph.withAttributes(supervisionStrategy(decider)).run()
  // @formatter:on
  impControl._1.onComplete{
    case Failure(t) =>
      logger.error("ImpStream: CommitOffsetsSink done with error", t)
    case _ =>
      logger.info("ImpStream: CommitOffsetsSink done without error")
  }

  impControl._2.onComplete{
    case Failure(t) =>
      logger.error("ImpStream: PublishParentsSink done with error", t)
    case _ =>
      logger.info("ImpStream: PublishParentsSink done without error")
  }

  def shutdown = {
    logger.warn(s"ImpSream requested to shutdown")
    val allDone = Future.sequence(List(impControl._1, impControl._2))

    if(!allDone.isCompleted)
      sharedKillSwitch.shutdown()

    Try {
      Await.ready(allDone, 10.seconds)
    }.recover {
      case t: Throwable =>
        logger.error(
          s"Imp stream failed to shutdown after waiting for 10 seconds."
        )
    }
    logger.warn("ImpStream is down")
  }

  private def checkParent(
                           parentWithChildDate: (String, DateTime)
                         ): Future[(Boolean, (String, DateTime))] = {
    if ((parentWithChildDate._1.trim != "$root") && (parentsCache.getIfPresent(
      parentWithChildDate._1
    ) eq null)) {

      val p = Promise[(Boolean, (String, DateTime))]()

      irwService.readPathUUIDA(parentWithChildDate._1).onComplete {
        case Failure(e) =>
          logger.error(
            s"failed retrieving parent [${parentWithChildDate._1}] uuid from irw, won't write it back to persist queue",
            e
          )
          p.success(true -> parentWithChildDate)
        case Success(BoxedFailure(e)) =>
          logger.error(
            s"failed retrieving parent [${parentWithChildDate._1}] uuid from irw (boxed failure), won't write it back to persist queue",
            e
          )
          p.success(true -> parentWithChildDate)
        case Success(EmptyBox) =>
          parentsCache.put(parentWithChildDate._1, "")
          p.success(true -> parentWithChildDate)
        case Success(_: FullBox[String]) =>
          parentsCache.put(parentWithChildDate._1, "")
          p.success(false -> parentWithChildDate)
      }

      p.future
    } else
      Future.successful(false -> parentWithChildDate)
  }
}
