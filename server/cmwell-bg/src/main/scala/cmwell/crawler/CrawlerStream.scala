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
import akka.stream.{ClosedShape, SourceShape}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Source}
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

import scala.concurrent.duration.{DurationInt, DurationLong}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

//case class CrawlerPosition(offset: Long, timeStamp: Long)
sealed trait CrawlerState
case class CrawlerBounds(offset: Long) extends CrawlerState
case class CrawlerMessage[T](offset: Long, msg: T)
case object NullCrawlerBounds extends CrawlerState

sealed trait DetectionResult
case object AllClear extends DetectionResult
case class CasError(details: String, original: SingleCommand) extends DetectionResult
case class EsError(details: String, original: SingleCommand) extends DetectionResult
case class GeneralError(details: String, original: SingleCommand) extends DetectionResult

object CrawlerStream extends LazyLogging {
  //todo: delay should be configurable
  val retryDuration = 30.seconds
  val safetyNetTimeInMillis: Long = ??? // todo: get from config
  case class CrawlerMaterialization(control: Consumer.Control, doneState: Future[Done])

  def createAndRunCrawlerStream(config: Config, topic: String, partition: Int)
                               (irwService: IRWService, ftsService: FTSServiceOps, zStore: ZStore, offsetsService: OffsetsService)
                               (implicit sys: ActorSystem, ec: ExecutionContext): CrawlerMaterialization = {

    def detectCas(offset: Long, path: String, timestamp: Long): Future[DetectionResult] = {
      irwService.lastVersion(path).map { case (ts,uuid) =>
        ??? // what do we want to do with last version?
      }.recover {
        case _: NoSuchElementException => // inconsistency detected! (no data in `path` table at all!)
        case t => logger.error(s"detect failed on path $path", t)
      }

      irwService.historyNeighbourhood(path, timestamp, desc = true, limit = 2).map { v =>
        // will return the version of timestamp `timestamp`, and the previous one
        if(v.length!=2) {
          zStore.getStringOpt(s"imp.${partition}_$offset").map {
            case Some("nu") => // Ok this was a null update
            case _ => // inconsistency detected!
          }
        } else {
          ??? // we have confirmed an update with timestamp `timestamp`, and we have the one before it. What's next?
        }
      }


      // todo filter system fields in cql...
      def getRawSystemFields(uuid: String) = irwService.rawReadUuidAsyc(uuid).map(_.filter(_._1 == "cmwell://meta/sys"))





      Future.successful(AllClear)
    }

    def detectEs(uuid: String, indexName: String, shouldBeCurrent: Boolean)(originalCommand: SingleCommand): Future[DetectionResult] = {
      def stringifyBoolean(b: Boolean) = if(b) " " else "not "

      ftsService.get(uuid, indexName).map { case (thinfoton, isActualCurrent) =>
          // should we examine f.path, f.lastModified etc. ?
        if(shouldBeCurrent == isActualCurrent)
          AllClear
        else
          EsError(s"[$uuid] is ${stringifyBoolean(isActualCurrent)}current, but expected to be ${stringifyBoolean(shouldBeCurrent)}current.", originalCommand)
      }.recover {
        case t: NoSuchElementException => EsError(t.getMessage, originalCommand)
        case t => logger.error(s"Something went wrong while getting uuid [$uuid] from ES", t); EsError("Could not retrieve ES info",  originalCommand)
      }
    }

    val bootStrapServers = config.getString("cmwell.bg.kafka.bootstrap.servers")
    val persistId = config.getString("cmwell.crawler.persist.key") + "." + partition + (if (!topic.endsWith(".priority")) ".p" else "") + "_offset"
    val initialPersistOffset = Try(offsetsService.readWithTimestamp(persistId))
    initialPersistOffset match {
      case Failure(ex) =>
        val failure = Future.failed[Done](
          new Exception(s"zStore read for initial offset failed! Failing the crawler stream. It should be automatically restarted later.", ex))
        CrawlerMaterialization(null, failure)
      case Success(persistedOffset) =>
        val initialOffset = persistedOffset.fold(0L)(_.offset)
        val offsetSrc = positionSource(topic, partition, offsetsService)(sys, ec)
        val nonBackpressuredMessageSrc = messageSource(initialOffset, topic, partition, bootStrapServers)(sys)
        val messageSrc = backpressuredMessageSource(offsetSrc, nonBackpressuredMessageSrc)
        messageSrc
            .map { msg =>
              val cmd = CommandSerializer.decode(msg.value) match {
                case CommandRef(ref) => zStore.get(ref).map(CommandSerializer.decode(_).asInstanceOf[SingleCommand])(ec)
                case singleCommand => Future.successful(singleCommand.asInstanceOf[SingleCommand])
              }
              CrawlerMessage(msg.offset(), cmd)
            }
            .mapAsync(1)(cm => cm.msg.map(CrawlerMessage(cm.offset, _))(ec))
//            .map(b => b.)

        ???
      //todo: the future[done] will combine isShutDown with the sink future done!!!
    }
  }
  private def positionSource(topic: String, partition: Int, offsetService: OffsetsService)
                            (sys: ActorSystem, ec: ExecutionContext): Source[Long, NotUsed] = {
    val startingState = PersistedOffset(-1, -1)
    val zStoreKeyForImp = "imp." + partition + (if (!topic.endsWith(".priority")) ".p" else "") + "_offset"
    val zStoreKeyForIndexer = "persistOffsetsDoneByIndexer." + partition + (if (!topic.endsWith(".priority")) ".p" else "") + "_offset"
    Source.unfoldAsync(startingState) { state =>
      val zStoreImpPosition: Option[PersistedOffset] = offsetService.readWithTimestamp(zStoreKeyForImp)
      val zStoreIndexerPosition: Option[PersistedOffset] = offsetService.readWithTimestamp(zStoreKeyForIndexer)
      val zStorePosition = for {
        impPosition <- zStoreImpPosition
        indexPosition <- zStoreIndexerPosition
      } yield PersistedOffset(Math.min(impPosition.offset, indexPosition.offset), Math.max(impPosition.timestamp, indexPosition.timestamp))
      zStorePosition.fold {
        logger.warn(s"zStore responded with None for key $zStoreKeyForImp or $zStoreKeyForIndexer. Not reasonable! Will retry again in $retryDuration.")
        akka.pattern.after(retryDuration, sys.scheduler)(Future.successful(Option(state -> (NullCrawlerBounds: CrawlerState))))(ec)
      } { position =>
        if (position.offset < state.offset) {
          val e = new Exception(s"Persisted offset [${position.offset}] is smaller than the current crawler offset [${state.offset}]. " +
            s"This should never happen. Closing crawler stream!")
          logger.error("This is no need to print an exception twice - just read it from the oncomplete log print!!!") //todo: implement in onComplete
          Future.failed[Option[(PersistedOffset, CrawlerState)]](e)
        }
        else if (position.offset == state.offset) {
          //Bg didn't do anything from the previous check - sleep and then emit some sentinel for another element
          akka.pattern.after(retryDuration, sys.scheduler)(Future.successful(Some(state -> NullCrawlerBounds)))(ec)
        }
        else {
          val now = System.currentTimeMillis()
          val timeDiff = now - position.timestamp
          val delayDuration = Math.max(0, safetyNetTimeInMillis - timeDiff).millis
          //todo: watch off by one errors!!
          val bounds = CrawlerBounds(position.offset)
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

  private def backpressuredMessageSource(offsetSource: Source[Long, NotUsed],
                                         messageSource: Source[ConsumerRecord[Array[Byte], Array[Byte]], Consumer.Control]) =
    Source.fromGraph(GraphDSL.create(offsetSource, messageSource)((a, b) => b) {
      implicit builder => {
        (offsetSource, msgSource) => {
          import akka.stream.scaladsl.GraphDSL.Implicits._
          val ot = builder.add(OffsetThrottler())
          offsetSource ~> ot.in0
          msgSource ~> ot.in1
          SourceShape(ot.out)
        }
      }
    })
}
