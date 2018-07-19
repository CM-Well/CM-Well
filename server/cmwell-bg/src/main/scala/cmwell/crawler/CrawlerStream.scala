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

package cmwell.crawler

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ClosedShape, SourceShape}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import cmwell.common._
import cmwell.common.formats.BGMessage
import cmwell.fts.{FTSServiceNew, FTSServiceOps}
import cmwell.irw.IRWService
import cmwell.zstore.ZStore
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import cmwell.util.concurrent.travector

//case class CrawlerPosition(offset: Long, timeStamp: Long)
sealed trait CrawlerState
case class CrawlerBounds(offset: Long) extends CrawlerState
case object NullCrawlerBounds extends CrawlerState

case class SystemField(name: String, value: String)
sealed trait CasVersion {
  val uuid: String
  val timestamp: Long
}
case class BareCasVersion(uuid: String, timestamp: Long) extends CasVersion
case class CasVersionWithSysFields(uuid: String, timestamp: Long, fields: Vector[SystemField]) extends CasVersion
case class PathsVersions(latest: Option[CasVersion], versions: Vector[CasVersion])
case class KafkaLocation(topic: String, partition: Int, offset: Long) {
  override def toString: String = s"[$topic, partition: $partition, offset: $offset]"
}
case class CommandWithLocation(cmd: SingleCommand, location: KafkaLocation)
sealed trait DetectionResult
//All is ok and there is nothing more to check
case class AllClear(cmdWithLocation: CommandWithLocation) extends DetectionResult
//so far it's ok - continue checking
case class SoFarClear(pathVersions: PathsVersions, cmdWithLocation: CommandWithLocation) extends DetectionResult
sealed trait DetectionError {
  val details: String
  val cmdWithLocation: CommandWithLocation
}
case class CasError(details: String, cmdWithLocation: CommandWithLocation) extends DetectionResult with DetectionError
case class EsBadCurrentError(details: String, cmdWithLocation: CommandWithLocation) extends DetectionResult with DetectionError
case class EsMissingUuidError(details: String, cmdWithLocation: CommandWithLocation) extends DetectionResult with DetectionError

object CrawlerStream extends LazyLogging {
  case class CrawlerMaterialization(control: Consumer.Control, doneState: Future[Done])
  private val systemFieldsNames = Set("dc", "indexName", "indexTime", "lastModified", "path", "type")

  def createAndRunCrawlerStream(config: Config, topic: String, partition: Int)
                               (irwService: IRWService, ftsService: FTSServiceOps, zStore: ZStore, offsetsService: OffsetsService)
                               (sys: ActorSystem, mat: ActorMaterializer, ec: ExecutionContext): CrawlerMaterialization = {

    //todo: check priority scenario - what will be the current version?


    val crawlerId = s"Crawler [$topic, partition: $partition]:"
    val bootStrapServers = config.getString("cmwell.bg.kafka.bootstrap.servers")
    val partitionId = partition + (if (topic.endsWith(".priority")) ".p" else "")
    val persistId = config.getString("cmwell.crawler.persist.key") + "." + partitionId + "_offset"
    val maxTime = config.getDuration("cmwell.crawler.persist.maxTime").toMillis.millis
    val maxAmount = config.getInt("cmwell.crawler.persist.maxAmount")
    val safetyNetTimeInMillis: Long = config.getDuration("cmwell.crawler.safetyNetTime").toMillis
    val retryDuration = config.getDuration("cmwell.crawler.retryDuration").getSeconds.seconds
    val checkParallelism = config.getInt("cmwell.crawler.checkParallelism")

    val initialPersistOffset = Try(offsetsService.readWithTimestamp(persistId))

    def checkKafkaMessage(msg: ConsumerRecord[Array[Byte], Array[Byte]]): Future[Long] = {
      kafkaMessageToSingleCommand(msg)
        .flatMap(getVersionsFromPathsTable)(ec)
        .flatMap(checkInconsistencyOfPathTableOnly)(ec)
        .flatMap(enrichVersionsWithSystemFields)(ec)
        .map(checkSystemFields)(ec)
        .flatMap(checkEsVersions)(ec)
        .map(reportErrors)(ec)
        .map(_ => msg.offset())(ec)
    }

    def kafkaMessageToSingleCommand(msg: ConsumerRecord[Array[Byte], Array[Byte]]) = {
      val kafkaLocation = KafkaLocation(topic, partition, msg.offset())
      CommandSerializer.decode(msg.value) match {
        case CommandRef(ref) => zStore.get(ref).map(cmd => CommandWithLocation(CommandSerializer.decode(cmd).asInstanceOf[SingleCommand], kafkaLocation))(ec)
        case singleCommand => Future.successful(CommandWithLocation(singleCommand.asInstanceOf[SingleCommand], kafkaLocation))
      }
    }

    def getVersionsFromPathsTable(cmdOffset: CommandWithLocation) = {
      implicit val localEc: ExecutionContext = ec
      val cmd = cmdOffset.cmd
      val latestVersion = irwService.lastVersion(cmd.path).map(v1 => v1.map(v2 => BareCasVersion(v2._2, v2._1)))(ec)
      val neighbourhoodVersions =
        irwService.historyNeighbourhood(cmd.path, cmd.lastModified.getMillis, desc = true, limit = 2).map(_.map(v => BareCasVersion(v._2, v._1)))(ec)
      for {
        latest <- latestVersion
        versions <- neighbourhoodVersions
      } yield {
        logger.trace(s"$crawlerId The CAS versions of offset: ${cmdOffset.location.offset} are: [latest: $latest] [versions: $versions]")
        (PathsVersions(latest, versions), cmdOffset)
      }
    }

    //checks that the given command is ok (either in paths table or null update or grouped command)
    def checkInconsistencyOfPathTableOnly(pathsCmdWithLocation: (PathsVersions, CommandWithLocation)) = {
      val (paths, cmdWithLocation@CommandWithLocation(cmd, location)) = pathsCmdWithLocation
      if (paths.latest.isEmpty)
        Future.successful[DetectionResult](CasError(s"No last version in paths table for path ${cmd.path}!", cmdWithLocation))
      else {
        //in case initial version of an infoton that was written several times fast, there will be a grouped commands without anything before.
        //Crawler needs to check whether this is the case (e.g. empty versions and the command is grouped)
        //todo: check the lastModified + 1 of BG. in some case there might be small shift of last modified and it will be ok.
        if (paths.versions.isEmpty || paths.versions.head.timestamp != cmd.lastModified.getMillis) {
          zStore.getStringOpt(s"imp.${partitionId}_${location.offset}").map {
            //the current offset was null update of grouped command. Bg didn't change anything in the system due to it - The check is finished in this stage
            case Some("nu" | "grp") =>
              AllClear(cmdWithLocation)
            case _ => CasError(s"command [path:${cmd.path}, last modified:${cmd.lastModified}] " +
              s"is not in paths table and it isn't null update or grouped command!", cmdWithLocation)
          }(ec)
        }
        //This offset's command exists. Still need to verify current and ES.
        else Future.successful(SoFarClear(paths, cmdWithLocation))
      }
    }

    lazy val fieldBreakOut = scala.collection.breakOut[Seq[(String, String, String)], SystemField, Vector[SystemField]]
    def getSystemFields(uuid: String) =
      irwService.rawReadSystemFields(uuid).map(_.collect { case (_, field, value) if field != "data" => SystemField(field, value) }(fieldBreakOut))(ec)

    def enrichVersionsWithSystemFields(previousResult: DetectionResult) = {
      previousResult match {
        case SoFarClear(PathsVersions(latestOpt, versions), cmdWithLocation) =>
          //todo: I am not sure getting the fields of latest is needed
          //val latest = latestOpt.get
          //val enrichedLatestFut = getSystemFields(latest.uuid).map(CasVersionWithSysFields(latest.uuid, latest.timestamp, _))
          val enrichedVersionsFut = travector(versions)(v => getSystemFields(v.uuid).map(CasVersionWithSysFields(v.uuid, v.timestamp, _))(ec))(ec)
          enrichedVersionsFut.map(enrichedVersions => SoFarClear(PathsVersions(latestOpt, enrichedVersions), cmdWithLocation))(ec)
        //for (l <- enrichedLatestFut; v <- enrichedVersionsFut) yield SoFarClear(PathsVersions(Some(l), v), cmdAndOffset)
        case other => Future.successful(other)
      }
    }

    def checkSystemFields(previousResult: DetectionResult) = {
      previousResult match {
        case prev@SoFarClear(PathsVersions(latestOpt, versions), cmdWithLocation) =>
          //The assumption is that the only version checked is the actual version written now.
          //(BG changes only ES current and nothing in CAS for previous versions)
          val analyzed = versions.head
          analyzed match {
            case CasVersionWithSysFields(uuid, timestamp, fields) =>
              logger.trace(s"$crawlerId The fields of [offset: ${cmdWithLocation.location.offset}, uuid: $uuid, timestamp: $timestamp] are: $fields")
              val badFields = fields.groupBy(_.name).collect {
                case (name, values) if values.length > 1 => s"field [$name] has too many values [${values.map(_.value).mkString(",")}]"
              }
              val analyzedFields = fields.map(_.name).toSet
              val missingFields = systemFieldsNames.filterNot(analyzedFields)
              if (missingFields.nonEmpty)
                CasError(s"system fields [${missingFields.mkString(",")}] are missing!", cmdWithLocation)
              else if (badFields.nonEmpty) {
                CasError(s"Duplicated system fields! ${badFields.mkString(";")}", cmdWithLocation)
              }
              else prev
            case _ => ??? //should never happen
          }
        case other => other
      }
    }

    def verifyEsCurrentState(uuid: String, indexName: String, shouldBeCurrent: Boolean)(previous: SoFarClear) =
      ftsService.get(uuid, indexName)(ec).map {
        case Some((_, isActualCurrent)) if isActualCurrent == shouldBeCurrent => previous
        case Some((_, isActualCurrent)) => EsBadCurrentError(s"uuid [$uuid] has unexpected current property of [$isActualCurrent]", previous.cmdWithLocation)
        case None => EsMissingUuidError(s"uuid $uuid doesn't exist in index $indexName.", previous.cmdWithLocation)
      }(ec)

    def checkFirstEsVersion(first: CasVersionWithSysFields)(previousResult: SoFarClear) = {
      val latest = previousResult.pathVersions.latest.get
      val shouldFirstBeCurrent = latest.timestamp == first.timestamp
      val firstIndexName = first.fields.find(_.name == "indexName").get.value
      verifyEsCurrentState(first.uuid, firstIndexName, shouldFirstBeCurrent)(previousResult).map {
        case _: EsBadCurrentError if !shouldFirstBeCurrent =>
          logger.info(s"The checked uuid [${first.uuid}] has current property of true but it's not the last version in CAS. " +
            s"It is probably that a newer version is being written to Cassandra and not yet updated in ES.")
          //The initial thought was to recheck it but in case of a long difference between imp and indexer it won't help.
          //And anyway it can't be an issue because an infoton is always written with current: true. Hence returning SoFarClear
          //val delayDuration = safetyNetTimeInMillis.millis
          //akka.pattern.after(delayDuration, sys.scheduler)(verifyEsCurrentState(first.uuid, firstIndexName, shouldFirstBeCurrent)(previousResult))(ec)
          previousResult
        case other => other
      }(ec)
    }

    def checkEsVersions(previousResult: DetectionResult) = {
      previousResult match {
        case prev@SoFarClear(PathsVersions(Some(_), Vector(first: CasVersionWithSysFields)), _) =>
          checkFirstEsVersion(first)(prev)
        case prev@SoFarClear(PathsVersions(Some(_), Vector(first: CasVersionWithSysFields, second: CasVersionWithSysFields)), _) =>
          val secondIndexNameOpt = second.fields.find(_.name == "indexName").map(_.value)
          checkFirstEsVersion(first)(prev)
            .flatMap { firstResult =>
              secondIndexNameOpt.fold(Future.successful(firstResult)) { secondIndexName =>
                verifyEsCurrentState(second.uuid, secondIndexName, shouldBeCurrent = false)(prev)
              }
            }(ec)
        case other => Future.successful(other)
      }
    }

    def reportErrors(finalResult: DetectionResult) = {
      finalResult match {
        case err: DetectionError =>
          logger.error(s"$crawlerId Inconsistency found: ${err.details} for command in location ${err.cmdWithLocation.location}. " +
            s"Original command: ${err.cmdWithLocation.cmd}")
        case other => other
      }
    }

    //the actual crawler stream starts here
    initialPersistOffset match {
      case Failure(ex) =>
        val failure = Future.failed[Done](
          new Exception(s"zStore read for initial offset failed! Failing the crawler stream. It should be automatically restarted later.", ex))
        CrawlerMaterialization(null, failure)
      case Success(persistedOffset) =>
        val initialOffset = persistedOffset.fold(0L)(_.offset + 1)
        logger.info(s"$crawlerId Starting the crawler with initial offset of $initialOffset")
        val offsetSrc = positionSource(crawlerId, partitionId, offsetsService, retryDuration, safetyNetTimeInMillis)(sys, ec)
        val nonBackpressuredMessageSrc = messageSource(initialOffset, topic, partition, bootStrapServers)(sys)
        val messageSrc = backpressuredMessageSource(crawlerId, offsetSrc, nonBackpressuredMessageSrc)
        messageSrc
          .mapAsync(checkParallelism)(checkKafkaMessage)
          .via(CrawlerRatePrinter(crawlerId, 500, 60000)(logger))
          //todo: We need only the last element. There might be a way without save all the elements. Also getting last can be time consuming
          .groupedWithin(maxAmount, maxTime)
          .map(_.last)
          .mapAsync(1)(offsetsService.writeAsync(persistId, _))
          .toMat(Sink.ignore) { (control, done) =>
            val allDone = done.flatMap { _ =>
              logger.info(s"$crawlerId The sink of the crawler of is done. Still waiting for the done signal of the control.")
              control.isShutdown.map { d =>
                logger.info(s"$crawlerId The control of the stream is completely done now. If the system is up it should be restarted later.")
                d
              }(ec)
            }(ec)
            allDone.onComplete {
              case Success(_) => //do nothing (the log prints are in the future itself)
              case Failure(ex) =>
                logger.error(s"$crawlerId The stream exited with an exception. " +
                  s"If the system is up it should be restarted later, please look in the main application log file. The exception was:", ex)
            }(ec)
            CrawlerMaterialization(control, allDone)
          }
          .run()(mat)
    }
  }

  private def positionSource(crawlerId: String, partitionId: String, offsetService: OffsetsService, retryDuration: FiniteDuration, safetyNetTimeInMillis: Long)
                            (sys: ActorSystem, ec: ExecutionContext): Source[Long, NotUsed] = {
    val startingState = PersistedOffset(-1, -1)
    val zStoreKeyForImp = "imp." + partitionId + "_offset"
    val zStoreKeyForIndexer = "persistOffsetsDoneByIndexer." + partitionId + "_offset"
    Source.unfoldAsync(startingState) { state =>
      val zStoreImpPosition: Option[PersistedOffset] = offsetService.readWithTimestamp(zStoreKeyForImp)
      val zStoreIndexerPosition: Option[PersistedOffset] = offsetService.readWithTimestamp(zStoreKeyForIndexer)
      val zStorePosition = for {
        impPosition <- zStoreImpPosition
        indexPosition <- zStoreIndexerPosition
      } yield PersistedOffset(Math.max(Math.min(impPosition.offset, indexPosition.offset) - 1, 0), Math.max(impPosition.timestamp, indexPosition.timestamp))
      zStorePosition.fold {
        logger.warn(s"$crawlerId zStore responded with None for key $zStoreKeyForImp or $zStoreKeyForIndexer. Not reasonable! " +
          s"Will retry again in $retryDuration.")
        akka.pattern.after(retryDuration, sys.scheduler)(Future.successful(Option(state -> (NullCrawlerBounds: CrawlerState))))(ec)
      } { position =>
        if (position.offset < state.offset) {
          val e = new Exception(s"Persisted offset [${position.offset}] is smaller than the current crawler offset [${state.offset}]. " +
            s"This should never happen. Closing crawler stream!")
          Future.failed[Option[(PersistedOffset, CrawlerState)]](e)
        }
        else if (position.offset == state.offset) {
          //Bg didn't do anything from the previous check - sleep and then emit some sentinel for another element
          logger.info(s"$crawlerId Got an offset ${position.offset} that is the same as the previous one. " +
            s"Will try again in $retryDuration")
          akka.pattern.after(retryDuration, sys.scheduler)(Future.successful(Some(state -> NullCrawlerBounds)))(ec)
        }
        else {
          val now = System.currentTimeMillis()
          val timeDiff = now - position.timestamp
          val delayDuration = Math.max(0, safetyNetTimeInMillis - timeDiff).millis
          //todo: watch off by one errors!!
          val bounds = CrawlerBounds(position.offset)
          logger.info(s"$crawlerId Got new max offset ${position.offset}. Setting up a safety net delay of $delayDuration before using it.")
          akka.pattern.after(delayDuration, sys.scheduler)(Future.successful(Some(position -> bounds)))(ec)
        }
      }
    }
      .collect {
        case bounds: CrawlerBounds => bounds.offset
      }
  }

  private def messageSource(initialOffset: Long, topic: String, partition: Int, bootStrapServers: String)
                           (sys: ActorSystem): Source[ConsumerRecord[Array[Byte], Array[Byte]], Consumer.Control] = {
    val consumerSettings = ConsumerSettings(sys, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootStrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val subscription = Subscriptions.assignmentWithOffset(
      new TopicPartition(topic, partition) -> initialOffset)
    Consumer.plainSource(consumerSettings, subscription)
  }

  private def backpressuredMessageSource(crawlerId: String,
                                         offsetSource: Source[Long, NotUsed],
                                         messageSource: Source[ConsumerRecord[Array[Byte], Array[Byte]], Consumer.Control]) =
    Source.fromGraph(GraphDSL.create(offsetSource, messageSource)((a, b) => b) {
      implicit builder => {
        (offsetSource, msgSource) => {
          import akka.stream.scaladsl.GraphDSL.Implicits._
          val ot = builder.add(OffsetThrottler(crawlerId))
          offsetSource ~> ot.in0
          msgSource ~> ot.in1
          SourceShape(ot.out)
        }
      }
    })
}
