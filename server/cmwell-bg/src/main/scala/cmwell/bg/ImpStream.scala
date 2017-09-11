/**
  * Copyright 2015 Thomson Reuters
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


package cmwell.bg

import java.util.concurrent.{ConcurrentHashMap, TimeoutException}

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ProducerMessage.Message
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerSettings, Subscriptions}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.contrib.PartitionWith
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Merge, MergePreferred, Partition, RunnableGraph, Sink, Unzip, Zip}
import akka.stream.{ActorMaterializer, ClosedShape, KillSwitches}
import cmwell.common.{Command, _}
import cmwell.common.formats.JsonSerializerForES
import cmwell.domain.{Infoton, ObjectInfoton}
import cmwell.fts._
import cmwell.irw.IRWService
import cmwell.tracking._
import cmwell.util.exceptions.ElementAlreadyExistException
import cmwell.util.{BoxedFailure, EmptyBox, FullBox}
import cmwell.util.concurrent.SimpleScheduler._
import cmwell.util.stream.GlobalVarStateHandler
import cmwell.zstore.ZStore
import com.datastax.driver.core.ConsistencyLevel
import com.google.common.cache.CacheBuilder
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala._
import com.codahale.metrics.{Counter => DropwizardCounter, Histogram => DropwizardHistogram, Meter => DropwizardMeter, Timer => DropwizardTimer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Requests
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Created by israel on 14/06/2016.
  */
class ImpStream(partition: Int, config: Config, irwService: IRWService, zStore: ZStore, ftsService: FTSServiceNew,
                offsetsService: OffsetsService, decider: Decider, kafkaConsumer: ActorRef)
               (implicit actorSystem: ActorSystem, executionContext: ExecutionContext,
                materializer: ActorMaterializer
               ) extends LazyLogging with DefaultInstrumented {

  lazy val redlog = LoggerFactory.getLogger("bg_red_log")
  lazy val heartbitLogger = LoggerFactory.getLogger("heartbeat_log")
  val parentsCache = CacheBuilder.newBuilder().maximumSize(4000 * 10).build[String, String]()
  val beforePersistedCache = new ConcurrentHashMap[String, Infoton]()

  val merger = Merger()

  val bootStrapServers = config.getString("cmwell.bg.kafka.bootstrap.servers")
  val persistCommandsTopic = config.getString("cmwell.bg.persist.commands.topic")
  val persistCommandsTopicPriority = persistCommandsTopic + ".priority"
  val indexCommandsTopic = config.getString("cmwell.bg.index.commands.topic")
  val indexCommandsTopicPriority = indexCommandsTopic + ".priority"
  val maxInfotonWeightToIncludeInCommand = config.getInt("cmwell.bg.maxInfotonWeightToIncludeInCommand")
  val defaultDC = config.getString("cmwell.dataCenter.id")
  val esActionsBulkSize = config.getInt("cmwell.bg.esActionsBulkSize") // in bytes
  val esActionsGroupingTtl = config.getInt("cmwell.bg.esActionsGroupingTtl") // ttl for bulk es actions grouping in ms
  val groupCommandsByPathSize = config.getInt("cmwell.bg.groupCommandsByPathSize") // # of commands to group by path
  val groupCommandsByPathTtl = config.getInt("cmwell.bg.groupCommandsByPathTtl") // timeout for the above grouping
  val maxDocsPerShard = config.getLong("cmwell.bg.maxDocsPerShard")

    logger info s"ImpStream starting with config:\n ${config.entrySet().asScala.collect{case entry if entry.getKey.startsWith("cmwell") => s"${entry.getKey} -> ${entry.getValue.render()}"}.mkString("\n")}"

  /** *** Metrics *****/
  val existingMetrics = metricRegistry.getMetrics.asScala
  val writeCommandsCounter: Counter = existingMetrics.get("WriteCommand Counter").
    map { m => new Counter(m.asInstanceOf[DropwizardCounter]) }.
    getOrElse(metrics.counter("WriteCommand Counter"))
  val updatePathCommandsCounter: Counter = existingMetrics.get("UpdatePathCommand Counter").
    map { m => new Counter(m.asInstanceOf[DropwizardCounter]) }.
    getOrElse(metrics.counter("UpdatePathCommand Counter"))
  val deletePathCommandsCounter: Counter = existingMetrics.get("DeletePathCommand Counter").
    map { m => new Counter(m.asInstanceOf[DropwizardCounter]) }.
    getOrElse(metrics.counter("DeletePathCommand Counter"))
  val deleteAttributesCommandsCounter: Counter = existingMetrics.get("DeleteAttributesCommand Counter").
    map { m => new Counter(m.asInstanceOf[DropwizardCounter]) }.
    getOrElse(metrics.counter("DeleteAttributesCommand Counter"))
  val overrideCommandCounter: Counter = existingMetrics.get("OverrideCommand Counter").
    map { m => new Counter(m.asInstanceOf[DropwizardCounter]) }.
    getOrElse(metrics.counter("OverrideCommand Counter"))
  val indexNewInfotonCommandCounter: Counter = existingMetrics.get("IndexNewCommand Counter").
    map { m => new Counter(m.asInstanceOf[DropwizardCounter]) }.
    getOrElse(metrics.counter("IndexNewCommand Counter"))
  val indexExistingCommandCounter: Counter = existingMetrics.get("IndexExistingCommand Counter").
    map { m => new Counter(m.asInstanceOf[DropwizardCounter]) }.
    getOrElse(metrics.counter("IndexExistingCommand Counter"))
  val mergeTimer: Timer = existingMetrics.get("Merge Timer").
    map { m => new Timer(m.asInstanceOf[DropwizardTimer]) }.getOrElse(metrics.timer("Merge Timer"))
  val commandMeter: Meter = existingMetrics.get("Commands Meter").map { m => new Meter(m.asInstanceOf[DropwizardMeter]) }.
    getOrElse(metrics.meter("Commands Meter"))
  val infotonCommandWeightHist: Histogram = existingMetrics.get("WriteCommand OverrideCommand Infoton Weight Histogram").
    map { m => new Histogram(m.asInstanceOf[DropwizardHistogram]) }.
    getOrElse(metrics.histogram("WriteCommand OverrideCommand Infoton Weight Histogram"))
  val indexingTimer: Timer = existingMetrics.get("Indexing Timer").map { m => new Timer(m.asInstanceOf[DropwizardTimer]) }.
    getOrElse(metrics.timer("Indexing Timer"))
  val casFullReadTimer: Timer = existingMetrics.get("CAS Full Read Timer").map { m => new Timer(m.asInstanceOf[DropwizardTimer]) }.
    getOrElse(metrics.timer("CAS Full Read Timer"))
  val casEmptyReadTimer: Timer = existingMetrics.get("CAS Empty Read Timer").map { m => new Timer(m.asInstanceOf[DropwizardTimer]) }.
    getOrElse(metrics.timer("CAS Empty Read Timer"))
  val nullUpdateCounter: Counter = existingMetrics.get("NullUpdate Counter").
    map { m => new Counter(m.asInstanceOf[DropwizardCounter]) }.
    getOrElse(metrics.counter("NullUpdate Counter"))
  val indexBulkSizeHist: Histogram = existingMetrics.get("Index Bulk Size Histogram").
    map { m => new Histogram(m.asInstanceOf[DropwizardHistogram]) }.
    getOrElse(metrics.histogram("Index Bulk Size Histogram"))

  /** ****************/

  val streamId = s"imp.${partition}"

  val startingOffset = offsetsService.read(s"${streamId}_offset").getOrElse(0L)
  val startingOffsetPriority = offsetsService.read(s"${streamId}.p_offset").getOrElse(0L)

    logger info s"ImpStream($streamId), startingOffset: $startingOffset, startingOffsetPriority: $startingOffsetPriority"

  @volatile var fuseOn = true

  val subscription = Subscriptions.assignmentWithOffset(
    new TopicPartition(persistCommandsTopic, partition) -> startingOffset
  )

  val prioritySubscription = Subscriptions.assignmentWithOffset(
    new TopicPartition(persistCommandsTopicPriority, partition) -> startingOffsetPriority
  )

  val sharedKillSwitch = KillSwitches.shared("persist-sources-kill-switch")

  val indexNameAndCountStateHandler = new GlobalVarStateHandler[(String,Long)](1,6)(() => {
    ftsService.latestIndexNameAndCountAsync(s"cm_well_p${partition}_*").flatMap {
      case Some((name, count)) => Future.successful(name -> count)
      case None => {
        logger info s"no indexes found for partition $partition, creating first one"
        ftsService.createIndex(s"cm_well_p${partition}_0").flatMap { createResponse =>
          if (createResponse.isAcknowledged) {
            logger info s"successfully created first index for partition $partition"
            scheduleFuture(5.seconds) {
              logger info "updating all aliases"
              ftsService.updateAllAlias().map { indicesAliasesResponse =>
                if (!indicesAliasesResponse.isAcknowledged) {
                  logger.warn(s"updateAllAlias was not acknowledged [$indicesAliasesResponse]")
                }
                s"cm_well_p${partition}_0" -> 0L
              }
            }
          }
          else
            Future.failed(new RuntimeException(s"failed to create first index for partition: $partition"))
        }.andThen {
          case Failure(error) => logger.error("failed to init ES index/alias, aborting !!!", error)
          case Success((n,c)) => logger.info(s"ImpStream($streamId), startingIndexName: $n, startingIndexCount: $c")
        }
      }
    }
  })


  val persistCommandsSource = Consumer.plainExternalSource[Array[Byte], Array[Byte]](kafkaConsumer, subscription).map { msg =>
      logger debug s"consuming next payload from persist commands topic @ ${msg.offset()}"
    val command = CommandSerializer.decode(msg.value())
      logger debug s"consumed command: $command"
    BGMessage[Command](CompleteOffset(msg.topic(), msg.offset()), command)
  }.via(sharedKillSwitch.flow)

  val priorityPersistCommandsSource = Consumer.plainExternalSource[Array[Byte], Array[Byte]](kafkaConsumer, prioritySubscription).map { msg =>
      logger debug s"consuming next payload from priority persist commands topic @ ${msg.offset()}"
    val command = CommandSerializer.decode(msg.value())
      logger debug s"consumed priority command: $command"
    BGMessage[Command](CompleteOffset(msg.topic(), msg.offset()), command)
  }.via(sharedKillSwitch.flow)

  val heartBitLog = Flow[BGMessage[Command]].keepAlive(60.seconds, () => BGMessage(HeartbitCommand.asInstanceOf[Command])).filterNot {
    case BGMessage(_, HeartbitCommand) =>
      heartbitLogger info "Imp alive !!!"
      true
    case _ => false
  }

  val byteArraySerializer = new ByteArraySerializer()

  val kafkaProducerSettings =
    ProducerSettings(actorSystem, byteArraySerializer, byteArraySerializer)
      .withBootstrapServers(bootStrapServers)

  val numOfCassandraNodes = config.getInt("cmwell.bg.num.of.cassandra.nodes")
  val irwReadConcurrency = Try {
    config.getInt("cmwell.bg.irwReadConcurrency")
  }.recover { case _: Throwable =>
    math.max(numOfCassandraNodes / 2, 2)
  }.get

  val irwWriteConcurrency = Try {
    config.getInt("cmwell.bg.irwWriteConcurrency")
  }.recover { case _: Throwable =>
    math.max(numOfCassandraNodes / 3, 2)
  }.get

  val commandRefsFetcher = Flow[BGMessage[Command]].mapAsync(irwReadConcurrency) {
    case bgMessage@BGMessage(_, CommandRef(ref)) =>
      zStore.get(ref).map { payload =>
        bgMessage.copy(message = CommandSerializer.decode(payload))
      }
  }

  // cast to SingleCommand while updating metrics
  val commandToSingle = Flow[BGMessage[Command]].map { bgMessage =>
    bgMessage.message match {
      case wc: WriteCommand =>
        writeCommandsCounter += 1
        infotonCommandWeightHist += wc.infoton.weight
      case oc: OverwriteCommand =>
        overrideCommandCounter += 1
        infotonCommandWeightHist += oc.infoton.weight
      case _: UpdatePathCommand => updatePathCommandsCounter += 1
      case _: DeletePathCommand => deletePathCommandsCounter += 1
      case _: DeleteAttributesCommand => deleteAttributesCommandsCounter += 1
      case _ =>
    }
    commandMeter.mark()
    bgMessage.copy(message = bgMessage.message.asInstanceOf[SingleCommand])
  }

  val breakOut2 = scala.collection.breakOut[Seq[BGMessage[SingleCommand]], SingleCommand, Seq[SingleCommand]]

  val groupCommandsByPath = Flow[BGMessage[SingleCommand]].groupedWithin(groupCommandsByPathSize,
    groupCommandsByPathTtl.milliseconds).mapConcat[BGMessage[(String, Seq[SingleCommand])]] { messages =>
    logger debug s"grouping commands: ${
      messages.map {
        _.message
      }
    }"
    messages.groupBy {
      _.message.path
    }.map { case (path, bgMessages) =>
      val offsets = bgMessages.flatMap(_.offsets)
      val commands = bgMessages.map(_.message)(breakOut2)
      BGMessage(offsets, path -> commands)
    }
  }

  val addLatestInfotons = Flow[BGMessage[(String, Seq[SingleCommand])]].mapAsync(irwReadConcurrency) { case bgMessage@BGMessage(_, (path, commands)) =>
    logger debug s"reading base infoton by path: $path"
    val start = System.currentTimeMillis()
    irwService.readPathAsync(path, ConsistencyLevel.QUORUM).map {
      case BoxedFailure(e) =>
        logger.error(s"readPathAsync failed for [$path]", e)
        bgMessage.copy(message = Option.empty[Infoton] -> commands)
      case box =>
        logger debug s"got base infoton for path: $path from irw: ${box.toOption}"
        val end = System.currentTimeMillis()
        if (box.isDefined)
          casFullReadTimer.update((end - start).millis)
        else
          casEmptyReadTimer.update((end - start).millis)
        bgMessage.copy(message = box.toOption -> commands)
    }
  }

  val addMerged = Flow[((String,Long),BGMessage[(Option[Infoton], Seq[SingleCommand])])].map {
    case ((currentIndexName,_),bgMessage@BGMessage(_, (existingInfotonOpt, commands))) =>
      beforePersistedCache.synchronized {
        val baseInfoton = Option(beforePersistedCache.get(commands.head.path)) match {
          case None =>
            logger debug s"baseInfoton for path: ${commands.head.path} not in cache"
            existingInfotonOpt
          case cachedInfotonOpt@Some(cachedInfoton) =>
            logger debug s"base infoton for path: ${commands.head.path} in cache: $cachedInfoton"
            existingInfotonOpt match {
              case None => cachedInfotonOpt
              case Some(existingInfoton) =>
                if (cachedInfoton.lastModified.getMillis < existingInfoton.lastModified.getMillis) {
                  logger debug s"cached infoton is newer then base infoton"
                  cachedInfotonOpt.map {
                    _.copyInfoton(lastModified = existingInfoton.lastModified)
                  }
                }
                else {
                  logger debug "base infoton is newer then cached infoton"
                  cachedInfotonOpt
                }
            }
        }
        logger debug s"merging existing infoton: $baseInfoton with commands: $commands"

        val mergedInfoton =
          if (baseInfoton.isDefined || commands.size > 1)
            mergeTimer.time(merger.merge(baseInfoton, commands))
          else
            merger.merge(baseInfoton, commands)
        mergedInfoton.merged.foreach { i =>
          beforePersistedCache.put(i.path, i.copyInfoton(indexName = currentIndexName))
          schedule(60.seconds) {
            beforePersistedCache.remove(i.path)
          }
        }
        bgMessage.copy(message = (baseInfoton -> mergedInfoton))
      }
  }

  val filterDups = Flow[BGMessage[(Option[Infoton], Infoton)]].filterNot { bgMessage =>
    val isDup = bgMessage.message._1.map {
      _.uuid
    }.getOrElse("").equals(bgMessage.message._2.uuid)
    logger debug s"filtering infoton due to same uuid: ${
      bgMessage.message._1.map {
        _.uuid
      }.getOrElse("")
    } equals ${bgMessage.message._2.uuid}"
    isDup
  }

  val commitVirtualParents = Producer.plainSink[Array[Byte], Array[Byte]](kafkaProducerSettings)

  val breakOut3 = scala.collection.breakOut[Seq[(Boolean, (String, DateTime))],
    ProducerRecord[Array[Byte],Array[Byte]], collection.immutable.Iterable[ProducerRecord[Array[Byte],Array[Byte]]]]

  val createVirtualParents = Flow[((String,Long),BGMessage[Seq[(IndexCommand, Option[DateTime])]])].mapAsync(1) {
    case ((currentIndexName,_),BGMessage(offsets, indexCommands)) =>
      logger debug s"checking missing parents for index commands: $indexCommands"

      val parentsWithChildDate = cmwell.util.collections.distinctBy {
        indexCommands.collect { case (indexCommand, Some(lastModified)) =>
          (Infoton.getParent(indexCommand.path) -> lastModified)
        }
      } {
        _._1
      }

      Future.traverse(parentsWithChildDate)(checkParent).map(_.collect { case (true, (path, childLastModified)) =>
        val infoton = ObjectInfoton(path = path, dc = defaultDC, lastModified = childLastModified, indexName = currentIndexName)
        val writeCommand = WriteCommand(infoton)
        val payload = CommandSerializer.encode(writeCommand)
        val topic =
          if(offsets.exists(_.topic.endsWith("priority")))
            persistCommandsTopicPriority
          else
            persistCommandsTopic
        new ProducerRecord[Array[Byte], Array[Byte]](topic, infoton.path.getBytes, payload)
      }(breakOut3)
      )
  }.mapConcat(identity)

  val persistInCas = Flow[((String,Long),BGMessage[(Option[Infoton], (Infoton, Seq[String]))])].mapAsync(irwWriteConcurrency) {
    case ((currentIndexName,_),BGMessage(offsets, (previous, (latest, trackingIds)))) =>
      val latestWithIndexName = latest.copyInfoton(indexName = currentIndexName)
      logger debug s"writing lastest infoton: $latestWithIndexName"
      irwService.writeAsync(latestWithIndexName, ConsistencyLevel.QUORUM).map { i =>
        if (previous.isEmpty) {
          val statusTracking = trackingIds.map {
            StatusTracking(_, 1)
          }
          val indexNewInfoton: (IndexCommand, Option[DateTime]) = IndexNewInfotonCommand(i.uuid, true, i.path, Some(i),
            i.indexName, statusTracking) -> Some(i.lastModified)
          List(BGMessage(offsets, Seq(indexNewInfoton)))
        } else {
          val statusTracking = trackingIds.map {
            StatusTracking(_, 2)
          }
          val indexNewInfoton: (IndexCommand, Option[DateTime]) = IndexNewInfotonCommand(i.uuid, true, i.path, Some(i),
            i.indexName, statusTracking) -> Some(i.lastModified)
          val indexExistingInfoton: (IndexCommand, Option[DateTime]) = IndexExistingInfotonCommand(previous.get.uuid,
            previous.get.weight, previous.get.path, previous.get.indexName, statusTracking) -> None
          val updatedOffsetsForNew = offsets.map{ o => PartialOffset(o.topic, o.offset, 1, 2) }
          val updatedOffsetsForExisting = offsets.map{ o => PartialOffset(o.topic, o.offset, 2, 2) }
          List(BGMessage(updatedOffsetsForNew, Seq(indexNewInfoton)),BGMessage(updatedOffsetsForExisting, Seq(indexExistingInfoton)))
        }
      }
  }.mapConcat(identity)

  val breakOut = scala.collection.breakOut[Iterable[IndexCommand], Message[Array[Byte], Array[Byte], Seq[Offset]], collection.immutable.Seq[Message[Array[Byte], Array[Byte], Seq[Offset]]]]

  val mergedCommandToKafkaRecord = Flow[BGMessage[(BulkIndexResult, Seq[IndexCommand])]].mapConcat {
    case BGMessage(offset, (bulkIndexResults, commands)) =>
      logger debug s"failed indexcommands to kafka records. bulkResults: $bulkIndexResults \n commands: $commands"
      val failedCommands = if (bulkIndexResults.isInstanceOf[RejectedBulkIndexResult])
        commands
      else {
        val failedUUIDs = bulkIndexResults.failed.map {
          _.uuid
        }.toSet
        commands.filter { cmd => failedUUIDs.contains(cmd.uuid) }
      }

      failedCommands.map { failedCommand =>
        val commandToSerialize = if (failedCommand.isInstanceOf[IndexNewInfotonCommand] &&
          failedCommand.asInstanceOf[IndexNewInfotonCommand].infotonOpt.get.weight > maxInfotonWeightToIncludeInCommand) {
          logger debug s"infoton weight: ${failedCommand.asInstanceOf[IndexNewInfotonCommand].infotonOpt.get.weight} of failed command exceeds max weight"
          failedCommand.asInstanceOf[IndexNewInfotonCommand].copy(infotonOpt = None)
        } else {
          failedCommand
        }

        val topic =
          if(offset.exists(_.topic.endsWith("priority")))
            indexCommandsTopicPriority
          else
            indexCommandsTopic
        Message(
          new ProducerRecord[Array[Byte], Array[Byte]](topic, commandToSerialize.path.getBytes,
            CommandSerializer.encode(commandToSerialize)),
          offset
        )
      }(breakOut)
  }

  val indexCommandsToKafkaRecords = Flow[BGMessage[Seq[IndexCommand]]].mapConcat {
    case BGMessage(offset, commands) =>
      commands.map { command =>
        val commandToSerialize =
          if (command.isInstanceOf[IndexNewInfotonCommand] &&
            command.asInstanceOf[IndexNewInfotonCommand].infotonOpt.get.weight > maxInfotonWeightToIncludeInCommand) {
            command.asInstanceOf[IndexNewInfotonCommand].copy(infotonOpt = None)
          } else
            command
        Message(
          new ProducerRecord[Array[Byte], Array[Byte]](indexCommandsTopic, commandToSerialize.path.getBytes,
            CommandSerializer.encode(commandToSerialize)),
          offset
        )
      }(breakOut)
  }

  val publishIndexCommands = Producer.flow[Array[Byte], Array[Byte], Seq[Offset]](kafkaProducerSettings).map {
    _.message.passThrough
  }

  var lastOffsetPersisted = startingOffset - 1
  var lastOffsetPersistedPriority = startingOffsetPriority - 1

  val doneOffsets = new java.util.TreeSet[Offset]()
  val doneOffsetsPriority = new java.util.TreeSet[Offset]()

  def mergeOffsets(doneOffsets: java.util.TreeSet[Offset], newOffsets: Seq[Offset]) = {
      logger debug s"merging doneOffsets:\n $doneOffsets \n with newOffsets:\n $newOffsets"
    val (completedOffsets, partialOffsets) = newOffsets.partition(_.isInstanceOf[CompleteOffset])
      logger debug s"completedOffsests:\n $completedOffsets \n partialOffsets:\n$partialOffsets"
    doneOffsets.addAll(completedOffsets.asJava)
      logger debug s"doneOffsets after adding all completed new offsets:\n$doneOffsets"
    partialOffsets.groupBy(_.offset).foreach { case (_, o) =>
        logger debug s"handling new partial offset: $o"
      if (o.size == 2) {
          logger debug s"two new partial offsets become one completed, adding to doneOffsets"
        doneOffsets.add(CompleteOffset(o.head.topic, o.head.offset))
      }
      else if(doneOffsets.contains(o.head)) {
          logger debug s"doneOffsets already contained 1 partial offset for ${o.head} removing it and adding completed instead"
        doneOffsets.remove(o.head)
        doneOffsets.add(CompleteOffset(o.head.topic, o.head.offset))
      } else {
          logger debug s"adding new partial ${o.head} to doneOffsets"
        doneOffsets.add(o.head)
      }
    }
      logger debug s"doneOffsets after adding partial new offsets:\n $doneOffsets"
  }

  val commitOffsets = Flow[Seq[Offset]].groupedWithin(6000, 3.seconds).toMat {
    Sink.foreach { offsetGroups =>
      doneOffsets.synchronized { // until a suitable concurrent collection is found
        val (offsets, offsetsPriority) = offsetGroups.flatten.partition(_.topic == persistCommandsTopic)
        logger debug s"offsets: $offsets"
        logger debug s"priority offsets: $offsetsPriority"

        if(offsets.size > 0) {
          var prev = lastOffsetPersisted
          mergeOffsets(doneOffsets, offsets)
          val it = doneOffsets.iterator()
          while ( {
            val next = if (it.hasNext) Some(it.next) else None
            val continue = next.fold(false)(o => o.isInstanceOf[CompleteOffset] && o.offset - prev == 1)
            if (continue)
              prev = next.get.offset
            continue
          }) {
            it.remove()
          }

          if (prev > lastOffsetPersisted) {
            logger debug s"prev: $prev is greater than lastOffsetPersisted: $lastOffsetPersisted"
            offsetsService.write(s"${streamId}_offset", prev + 1L)
            lastOffsetPersisted = prev
          }
        }

        if(offsetsPriority.size > 0) {
          var prevPriority = lastOffsetPersistedPriority
          mergeOffsets(doneOffsetsPriority, offsetsPriority)
          val itPriority = doneOffsetsPriority.iterator()
          while ( {
            val next = if (itPriority.hasNext) Some(itPriority.next) else None
            val continue = next.fold(false)(o => o.isInstanceOf[CompleteOffset] && o.offset - prevPriority == 1)
            if (continue)
              prevPriority = next.get.offset
            continue
          }) {
            itPriority.remove()
          }

          if (prevPriority > lastOffsetPersistedPriority) {
            logger debug s"prevPriority: $prevPriority is greater than lastOffsetPersistedPriority: $lastOffsetPersistedPriority"
            offsetsService.write(s"${streamId}.p_offset", prevPriority + 1L)
            lastOffsetPersistedPriority = prevPriority
          }
        }

      }
    }
  }(Keep.right)

  val impGraph = RunnableGraph.fromGraph(GraphDSL.create(priorityPersistCommandsSource, persistCommandsSource, commitOffsets)((_, _, d) => d) { implicit builder =>
    (prioritySource, batchSource, sink) =>
      import GraphDSL.Implicits._

      val globalIndexNameCountState = builder.add(indexNameAndCountStateHandler)

      val preAddMergedZip = builder.add(Zip[(String,Long),BGMessage[(Option[Infoton], Seq[SingleCommand])]]())

      val preCreateVirtualParentsZip = builder.add(Zip[(String,Long),BGMessage[Seq[(IndexCommand, Option[DateTime])]]]())

      val prePersistInCasZip = builder.add(Zip[(String,Long),BGMessage[(Option[Infoton], (Infoton, Seq[String]))]]())

      val preProcessOverrideInfotonsWithStateZip = builder.add(Zip[(String,Long),BGMessage[(Seq[(Infoton,Option[String])],Option[Infoton])]]())

      val preIndexCommandsToESActionsZip = builder.add(Zip[(String,Long),BGMessage[Seq[IndexCommand]]]())

      val preManageIndicesZip = builder.add(Zip[(String,Long),BGMessage[(BulkIndexResult, Seq[IndexCommand])]]())

      val mergePrefferedSources = builder.add(
        MergePreferred[BGMessage[Command]](1, true)
      )


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

      // CommandRef goes left, all rest go right
      // update metrics for each type of command
      val commandsPartitioner = builder.add(
        Partition[BGMessage[Command]](
          2, {
            case BGMessage(_, CommandRef(_)) => 0
            case _ => 1
          }
        )
      )

      val partitionMerged = builder.add(
        Partition[BGMessage[(Option[Infoton], MergeResponse)]](2, {
          case BGMessage(_, (_, mergeResponse)) if mergeResponse.isInstanceOf[RealUpdate] => 0
          case _ => 1
        })
      )

      val reportNullUpdates = builder.add(
        Flow[BGMessage[(Option[Infoton], MergeResponse)]].mapAsync(1) {
          case BGMessage(offset, (_, NullUpdate(path, tids, evictions))) =>
            nullUpdateCounter += 1
            val nullUpdatesReportedAsDone = {
              if (tids.nonEmpty)
                TrackingUtilImpl.updateSeq(path, tids.map(StatusTracking(_, 1))).map {
                  case LogicalFailure(reason) =>
                    logger warn s"Failed to report tracking due to: $reason"
                    Future.successful(Unit)
                  case _ => Future.successful(Unit)
                }
              else
                Future.successful(())
            }
            val evictionsReported = {
              Future.traverse(evictions) {
                case (reason, Some(tid)) => TrackingUtilImpl.updateEvicted(path, tid, reason).map {
                  case LogicalFailure(reason) =>
                    logger warn s"Failed to report eviction tracking due to: $reason"
                    Future.successful(Unit)
                  case _ => Future.successful(Unit)
                }
                case (reason, None) => Future(logger info s"evicted command in path:$path because of: $reason")
              }
            }

            val offsetFuture = for {
              _ <- nullUpdatesReportedAsDone
              _ <- evictionsReported
            } yield offset

            offsetFuture
        }
      )

      val partitionNonNullMerged = builder.add(
        PartitionWith[BGMessage[(Option[Infoton], MergeResponse)],
          BGMessage[(Option[Infoton], (Infoton, Seq[String]))],
          (String, Seq[(String, Option[String])])] {
          case BGMessage(offset, (prev, RealUpdate(cur, tids, evicted))) =>
            if (evicted.nonEmpty)
              Right(cur.path -> evicted)
            else
              Left(BGMessage(offset, (prev, (cur, tids))))
        }
      )

      val singleCommandsMerge = builder.add(Merge[BGMessage[Command]](2))

      val overrideCommandsToInfotons = builder.add(
        Flow[BGMessage[(String, Seq[SingleCommand])]].mapAsync(irwReadConcurrency) {
          case bgMessage@BGMessage(_, (path, commands)) =>
            logger debug s"in override flow: going to read history for path: $path"
            irwService.historyAsync(path, 10000).map { v =>
              val existingUuids: Set[String] = v.map {
                _._2
              }(collection.breakOut)
              val filteredInfotons = commands.collect { case overwrite: OverwriteCommand if
              !existingUuids.contains(overwrite.infoton.uuid) => (overwrite.infoton, overwrite.trackingID)
              }
              bgMessage.copy(message = (path, filteredInfotons))
            }
        }
      )

      val partitionOverrideInfotons = builder.add(
        PartitionWith[BGMessage[
          (String, Seq[(Infoton, Option[String])])],
          BGMessage[(String, Seq[(Infoton, Option[String])])],
          Seq[Offset]] {
          case bgMessage@BGMessage(offset, (_, infotons)) =>
            if (infotons.nonEmpty) Left(bgMessage)
            else Right(offset)
        }
      )

      val processOverrideInfotons = builder.add(
        Flow[BGMessage[(String, Seq[(Infoton, Option[String])])]].mapAsync[BGMessage[(Seq[(Infoton,Option[String])],Option[Infoton])]](irwReadConcurrency) {
          case bgMessage@BGMessage(_, (path, infotons)) =>
            val sortedInfotons = infotons.sortWith { (l, r) =>
              if (l._1.lastModified.getMillis != r._1.lastModified.getMillis)
                l._1.lastModified.getMillis > r._1.lastModified.getMillis
              else
                l._1.uuid < r._1.uuid
            }
            irwService.readPathAsync(path, ConsistencyLevel.QUORUM).map {
              case BoxedFailure(e) =>
                logger.error(s"readPathAsync failed for [$path]", e)
                bgMessage.copy(message = (sortedInfotons, None))
              case b =>
                val infoOpt = b.toOption
                logger debug s"readPathAsync for path:$path returned: ${b.toOption}"
                bgMessage.copy(message = (sortedInfotons, infoOpt))
            }
        })

      val processOverrideInfotonsWithState = builder.add(
        Flow[((String,Long),BGMessage[(Seq[(Infoton,Option[String])],Option[Infoton])])].mapAsync(irwWriteConcurrency) {
          case ((currentIndexName,_),BGMessage(offsets, (newInfotons, existingInfotonOpt))) =>
            val newInfotonsWithIndexName = newInfotons.map {
              case (i, tid) =>
                i.copyInfoton(indexName = currentIndexName) -> tid
            }
            irwService.writeSeqAsync(newInfotonsWithIndexName.map(_._1), ConsistencyLevel.QUORUM).map { writtenInfotons =>
              val bgMessages: List[BGMessage[Seq[IndexCommand]]] = newInfotonsWithIndexName.toList match {
                case ((headInfoton, headTrackingId) :: tail) =>
                  val isHeadCurrent = existingInfotonOpt.isEmpty || {
                    if (headInfoton.lastModified.getMillis != existingInfotonOpt.get.lastModified.getMillis)
                      headInfoton.lastModified.getMillis > existingInfotonOpt.get.lastModified.getMillis
                    else
                      headInfoton.uuid < existingInfotonOpt.get.uuid
                  }
                  val numOfParts = if (existingInfotonOpt.isDefined) 2 else 1
                  val indexNewInfotonCommands: Seq[IndexCommand] = IndexNewInfotonCommand(headInfoton.uuid,
                    isHeadCurrent, headInfoton.path, Some(headInfoton), headInfoton.indexName,
                    Seq(headTrackingId).flatten.map {
                      StatusTracking(_, numOfParts)
                    }) :: tail.map { case (i, t) =>
                    IndexNewInfotonCommand(i.uuid, false, i.path, Some(i), i.indexName,
                      Seq(t).flatten.map {
                        StatusTracking(_, numOfParts)
                      })
                  }
                  existingInfotonOpt.fold{
                    List(BGMessage(offsets, indexNewInfotonCommands))
                  } { existingInfoton =>
                    val statusTracking = newInfotonsWithIndexName.collect {
                      case (_,Some(s)) => StatusTracking(s, 2)
                    }

                    if (isHeadCurrent) {
                      val updatedOffsetsForNew = offsets.map{ o => PartialOffset(o.topic, o.offset, 1, 2) }
                      val updatedOffsetsForExisting = offsets.map{ o => PartialOffset(o.topic, o.offset, 2, 2) }
                      val indexNew = BGMessage(updatedOffsetsForNew, indexNewInfotonCommands)
                      val indexExisting:BGMessage[Seq[IndexCommand]] = BGMessage(
                        updatedOffsetsForExisting,
                        Seq(IndexExistingInfotonCommand(existingInfoton.uuid, existingInfoton.weight,
                              existingInfoton.path, existingInfoton.indexName, statusTracking))
                      )
                      List(indexExisting, indexNew)
                    } else {
                      List(BGMessage(offsets, indexNewInfotonCommands))
                    }
                  }
                case _ => ???
              }

              logger debug s"override flow produced IndexCommands:\n$bgMessages"

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
        Flow[((String,Long),BGMessage[Seq[IndexCommand]])].map { case ((currentIndexName,_),bgMessage@BGMessage(_, indexCommands)) =>
          //TODO: remove after all envs were upgraded
          val fixedIndexCommands = indexCommands.map { indexCommand =>
            if (indexCommand.indexName != "")
              indexCommand
            else {
              indexCommand match {
                case inic: IndexNewInfotonCommand => inic.copy(indexName = currentIndexName)
                case ieic: IndexExistingInfotonCommand => ieic.copy(indexName = currentIndexName)
              }
            }

          }
          val actionRequests = fixedIndexCommands.flatMap {
            case IndexNewInfotonCommand(_, isCurrent, _, Some(infoton), indexName, trackingIDs) =>
              // count it for metrics
              indexNewInfotonCommandCounter += 1
              List(
                Try {
                  val indexTime = if (infoton.indexTime.isDefined) None else Some(System.currentTimeMillis())
                  val infotonWithUpdatedIndexTime =
                    if (indexTime.isDefined)
                      infoton.replaceIndexTime(indexTime.get)
                    else
                      infoton
                  val serializedInfoton = JsonSerializerForES.encodeInfoton(infotonWithUpdatedIndexTime, isCurrent)
                  // TODO: remove after all envs upgraded
                  val iName = if (indexName != "") indexName else currentIndexName
                  (ESIndexRequest(
                    Requests.indexRequest(iName).`type`("infoclone").id(infoton.uuid).create(true)
                      .source(serializedInfoton),
                    indexTime
                  ), infotonWithUpdatedIndexTime.weight)
                }
              ).filter {
                case Success(_) => true
                case Failure(t) =>
                  redlog info("failed to encode indexnewInfotonCommand's infoton, skipping it", t)
                  false
              }.map {
                _.get
              }
            case IndexExistingInfotonCommand(uuid, weight, _, indexName, _) =>
              //count it for metrics
              indexExistingCommandCounter += 1
              List((ESIndexRequest(
                new UpdateRequest(indexName, "infoclone", uuid).doc(s"""{"system":{"current": false}}""").version(1),
                None
              ), weight))
            case _ => ???
          }
          logger debug s"creating esactions: $actionRequests from index commands"
          bgMessage.copy(message = (actionRequests, fixedIndexCommands))
        }.filter {
          _.message._1.nonEmpty
        }
      )

      import cmwell.util.concurrent._
      val sendActionsToES = builder.add(
        Flow[Seq[BGMessage[(Seq[(ESIndexRequest, Long)], Seq[IndexCommand])]]].mapAsync(1) {
          case bgMessages =>
            val actions = bgMessages.flatMap(_.message._1.map(_._1))
            logger debug s"sending ${actions.size} actions to elasticsearch: ${actions} from commands: ${bgMessages.flatMap(_.message._2)}"
            indexBulkSizeHist += actions.size
            indexingTimer.timeFuture(ftsService.executeIndexRequests(actions).recover {
              case _: TimeoutException =>
                RejectedBulkIndexResult("Future Timedout")
            }
            ).map { bulkIndexResult =>
              // filter out expected es failures (due to we're at least once and not exactly once)
              val filteredResults = bulkIndexResult match {
                case s@SuccessfulBulkIndexResult(_, failed) if failed.nonEmpty =>
                  s.copy(failed = failed.filterNot { f =>
                    f.reason.startsWith("DocumentAlreadyExistsException") ||
                    f.reason.startsWith("VersionConflictEngineException")
                  })
                case other => other
              }
              BGMessage(bgMessages.flatMap(_.offsets), (filteredResults, bgMessages.flatMap(_.message._2)))
            }
        }
      )

      val manageIndices = builder.add(
        Flow[((String,Long),BGMessage[(BulkIndexResult, Seq[IndexCommand])])].mapAsync(1) {
          case (unchangedState@(currentIndexName,indexCount),bgMessage@BGMessage(_, (bulkRes, commands))) =>
            val numOfShardPerIndex = ftsService.numOfShardsForIndex(currentIndexName)
            var startingIndexCount = indexCount + bulkRes.successful.size
            if (startingIndexCount / numOfShardPerIndex >= maxDocsPerShard) {
              val (pref, suf) = currentIndexName.splitAt(currentIndexName.lastIndexOf('_') + 1)
              val nextCount = suf.toInt + 1
              val nextIndexName = pref + nextCount
              cmwell.util.concurrent.retry(3)(ftsService.createIndex(nextIndexName)).flatMap { _ =>
                scheduleFuture(10.seconds)(ftsService.updateAllAlias)
              }.map { case _ =>
                ((nextIndexName,0L),bgMessage)
              }
            } else
              Future.successful(unchangedState -> bgMessage)
        }
      )

      val unzipManageIndices = builder.add(Unzip[(String,Long),BGMessage[(BulkIndexResult,Seq[IndexCommand])]]())

      val updateIndexInfoInCas = builder.add(
        Flow[BGMessage[(BulkIndexResult, Seq[IndexCommand])]].mapAsync(math.max(numOfCassandraNodes / 3, 2)) {
          case bgMessage@BGMessage(_, (bulkRes, commands)) =>
            logger debug s"updating index info in cas for bulkRes: $bulkRes from commands: $commands"
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
            logger debug s"Partitioning index results: $bulkIndexResult from commands: $commands"
            if (bulkIndexResult.isInstanceOf[RejectedBulkIndexResult]) {
              logger info "ftservice rejected, turning fusing off and scheduling back on in 3 minutes"
              fuseOn = false
              cmwell.util.concurrent.SimpleScheduler.schedule(3.minutes) {
                logger info "turning fusing on after 3 minutes wait"
                fuseOn = true
              }
            }
            if (bulkIndexResult.isInstanceOf[RejectedBulkIndexResult] || bulkIndexResult.failed.size > 0) {
              logger debug s"BulkIndexResult:${bulkIndexResult.isInstanceOf[RejectedBulkIndexResult]} failed size:${bulkIndexResult.failed.size}"
              1
            } else {
              logger debug "no failures in BulkIndexResult"
              0
            }
        })
      )

      val fusePartition = builder.add(
        Partition[BGMessage[Seq[IndexCommand]]](2, {
          case bgMessage => if (fuseOn) 0 else 1
        })
      )

      val reportProcessTracking = builder.add(
        Flow[BGMessage[(BulkIndexResult, Seq[IndexCommand])]].mapAsync(3) {
          case BGMessage(offset, (_, indexCommands)) =>
            logger debug s"report process tracking of indexCommands: $indexCommands"
            Future.traverse(indexCommands) { indexCommand =>
              TrackingUtilImpl.updateSeq(indexCommand.path, indexCommand.trackingIDs).map {
                case LogicalFailure(reason) =>
                  logger warn s"Failed to report tracking for path: ${indexCommand.path} with tids: ${indexCommand.trackingIDs.mkString(",")} due to: $reason"
                  Future.successful(Unit)
                case _ => Future.successful(Unit)
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
                  logger warn s"Failed to report eviction tracking due to: $reason"
                  Future.successful(Unit)
                case _ => Future.successful(Unit)
              }
              case (reason, None) => Future(logger info s"evicted command in path:$path because of: $reason")
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

      prioritySource ~> mergePrefferedSources.preferred

      batchSource ~> mergePrefferedSources.in(0)

      mergePrefferedSources.out ~> heartBitLog ~> commandsPartitioner.in

      commandsPartitioner.out(0) ~> commandRefsFetcher ~> singleCommandsMerge.in(0)

      commandsPartitioner.out(1) ~> singleCommandsMerge.in(1)

      singleCommandsMerge.out ~> commandToSingle ~> singleCommandsPartitioner

      singleCommandsPartitioner.out(0) ~> groupCommandsByPath ~> overrideCommandsToInfotons ~> partitionOverrideInfotons.in

      globalIndexNameCountState.outlets(3) ~> preProcessOverrideInfotonsWithStateZip.in0

      partitionOverrideInfotons.out0 ~> processOverrideInfotons ~> preProcessOverrideInfotonsWithStateZip.in1

      preProcessOverrideInfotonsWithStateZip.out ~> processOverrideInfotonsWithState ~> indexCommandsMerge.in(0)

      partitionOverrideInfotons.out1 ~> mergeCompletedOffsets.in(0)

      globalIndexNameCountState.outlets(0) ~> preAddMergedZip.in0

      singleCommandsPartitioner.out(1) ~> groupCommandsByPath ~> addLatestInfotons ~> preAddMergedZip.in1

      preAddMergedZip.out ~> addMerged ~> partitionMerged.in

      partitionMerged.out(0) ~> partitionNonNullMerged.in

      globalIndexNameCountState.outlets(2) ~> prePersistInCasZip.in0

      partitionNonNullMerged.out0 ~> prePersistInCasZip.in1

      prePersistInCasZip.out ~> persistInCas ~> nonOverrideIndexCommandsBroadcast.in

      partitionNonNullMerged.out1 ~> logOrReportEvicted ~> Sink.ignore

      partitionMerged.out(1) ~> reportNullUpdates ~> mergeCompletedOffsets.in(1)

      globalIndexNameCountState.outlets(1) ~> preCreateVirtualParentsZip.in0

      nonOverrideIndexCommandsBroadcast.out(0) ~> preCreateVirtualParentsZip.in1

      preCreateVirtualParentsZip.out ~> createVirtualParents ~> commitVirtualParents

      nonOverrideIndexCommandsBroadcast.out(1) ~> removeLastModified ~> indexCommandsMerge.in(1)

      indexCommandsMerge.out ~> fusePartition

      globalIndexNameCountState.outlets(4) ~> preIndexCommandsToESActionsZip.in0

      fusePartition.out(0) ~> preIndexCommandsToESActionsZip.in1

      globalIndexNameCountState.outlets(5) ~> preManageIndicesZip.in0

      preIndexCommandsToESActionsZip.out ~> indexCommandsToESActions ~> groupEsActions ~> filterNonEmpty ~> sendActionsToES ~> preManageIndicesZip.in1

      preManageIndicesZip.out ~> manageIndices ~> unzipManageIndices.in

      unzipManageIndices.out0 ~> globalIndexNameCountState.inlets(0)

      unzipManageIndices.out1 ~> updateIndexInfoInCas ~> partitionIndexResult.in

      fusePartition.out(1) ~> indexCommandsToKafkaRecords ~> mergeKafkaRecords.in(0)

      // In case of partial success of indexing, failed goes to index_topic and successful offset are not reported
      // Since we have one offset (lowest) per group of commands. When the failed will succeed in Indexer it will be
      // reported
      partitionIndexResult.out(0) ~> reportProcessTracking ~> mergeCompletedOffsets.in(2)

      partitionIndexResult.out(1) ~> mergedCommandToKafkaRecord ~> mergeKafkaRecords.in(1)

      mergeKafkaRecords.out ~> publishIndexCommands ~> mergeCompletedOffsets.in(3)

      mergeCompletedOffsets.out ~> sink

      ClosedShape

  }
  )

  val impControl = impGraph.withAttributes(supervisionStrategy(decider)).run()

  impControl.onComplete {
    case Failure(t) =>
      logger error("imp stream stopped abnormally", t)
    case Success(_) => logger info "imp stream stopped normally"
  }

  def shutdown = {
    sharedKillSwitch.shutdown()
  }

  lazy val elementAlreadyExistException = new ElementAlreadyExistException("parent is already ingested")

  private def checkParent(parentWithChildDate: (String, DateTime)): Future[(Boolean, (String, DateTime))] = {
    if ((parentWithChildDate._1.trim != "$root") && (parentsCache.getIfPresent(parentWithChildDate._1) eq null)) {

      val p = Promise[(Boolean, (String, DateTime))]()

      irwService.readPathUUIDA(parentWithChildDate._1).onComplete {
        case Failure(e) =>
          logger.error(s"failed retrieving parent [${parentWithChildDate._1}] uuid from irw, won't write it back to persist queue", e)
          p.success(true -> parentWithChildDate)
        case Success(BoxedFailure(e)) =>
          logger.error(s"failed retrieving parent [${parentWithChildDate._1}] uuid from irw (boxed failure), won't write it back to persist queue", e)
          p.success(true -> parentWithChildDate)
        case Success(EmptyBox) =>
          parentsCache.put(parentWithChildDate._1, "")
          p.success(true -> parentWithChildDate)
        case Success(_: FullBox[String]) =>
          parentsCache.put(parentWithChildDate._1, "")
          p.success(false -> parentWithChildDate)
      }

      p.future
    }
    else Future.successful(false -> parentWithChildDate)
  }
}
