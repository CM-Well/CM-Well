package cmwell.analytics.downloader

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import cmwell.analytics.data.{DataWriter, ObjectExtractor}
import cmwell.analytics.util.{EsTopology, HttpUtil, Shard}
import com.typesafe.config.ConfigFactory
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.LogManager

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Promise}
import scala.util.{Failure, Random, Success}

object PartitionedDownloader {

  private val logger = LogManager.getLogger(PartitionedDownloader.getClass)

  private val config = ConfigFactory.load
  private val maxContentLength = config.getMemorySize("akka.http.client.parsing.max-content-length").toBytes
  private val maxFetchSize = config.getInt("extract-index-from-es.maximum-fetch-size")
  private val maxReadAttempts = config.getInt("extract-index-from-es.max-read-attempts")

  /** Create a Source that produces a stream of objects from a given shard. */
  private def infotonsFromShard[T <: GenericRecord](shard: Shard,
                                                    httpAddress: String,
                                                    currentOnly: Boolean,
                                                    extractor: ObjectExtractor[T],
                                                    format: String = "parquet",
                                                    sourceFilter: Boolean)
                                                   (implicit system: ActorSystem,
                                                    executionContext: ExecutionContextExecutor,
                                                    actorMaterializer: ActorMaterializer): Source[T, NotUsed] = {

    // This will be fetching from a single shard at a time, so there is no multiplier by the number of shards.
    // Calculate a fetch size that will fit within the maximum-content-length.
    val fetchSize = 1 max (maxContentLength / extractor.infotonSize / 1.1).toInt min maxFetchSize // 10% margin of error

    case class ScrollState(scrollId: Option[String] = None,
                           fetched: Long = 0) {

      def isInitialRequest: Boolean = scrollId.isEmpty

      def request: HttpRequest = scrollId.fold(initialRequest)(subsequentRequest)

      private val query =
        if (sourceFilter)
          s"{${extractor.filter(currentOnly)},${extractor.includeFields}}"
        else
          s"{${extractor.filter(currentOnly)}}" // Omit as a workaround for some broken shards

      private def initialRequest = HttpRequest(
        method = HttpUtil.SAFE_POST,
        uri = s"http://$httpAddress/${shard.indexName}/_search" +
          s"?scroll=1m" +
          s"&search_type=scan" +
          s"&size=$fetchSize" +
          s"&preference=_shards:${shard.shard}",
        entity = ByteString(query))

      private def subsequentRequest(scrollId: String) = HttpRequest(
        method = HttpUtil.SAFE_POST,
        uri = s"http://$httpAddress/_search/scroll?scroll=1m",
        entity = HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, ByteString(scrollId)))
    }

    Source.unfoldAsync(ScrollState()) { scrollState: ScrollState =>

      if (scrollState.isInitialRequest)
        logger.info(s"Requesting initial _scroll_id for index:${shard.indexName}, shard:${shard.shard} from:$httpAddress.")
      else
        logger.info(s"Requesting next $fetchSize uuids for index:${shard.indexName}, shard:${shard.shard} from:$httpAddress.")

      HttpUtil.jsonResultAsync(scrollState.request, "fetch next block").map { json =>

        val nextScrollId = json.findValue("_scroll_id").asText

        val objects = json.findValue("hits").findValue("hits").iterator.asScala.map(extractor.extractFromJson).toVector
        val objectsFetched = objects.length

        if (scrollState.isInitialRequest) {
          assert(objects.isEmpty)
          logger.info(s"Received initial _scroll_id.")
        } else {
          logger.info(s"Received $objectsFetched infotons. Total retrieved: ${scrollState.fetched + objectsFetched}.")
        }

        if (objects.isEmpty && !scrollState.isInitialRequest) {
          None
        } else {
          val nextScrollState = scrollState.copy(
            scrollId = Some(nextScrollId),
            fetched = scrollState.fetched + objectsFetched)

          Some(nextScrollState -> objects)
        }
      }
    }
      .map(Source(_)).flatMapConcat(identity)
  }


  /** Get a stream of records from an ES index, and pump them into a data writer sink.
    * Each shard from the source is processed in parallel.
    */
  def runDownload[T <: GenericRecord](esTopology: EsTopology,
                                      parallelism: Int,
                                      currentOnly: Boolean,
                                      objectExtractor: ObjectExtractor[T],
                                      dataWriterFactory: Shard => DataWriter[T],
                                      sourceFilter: Boolean)
                                     (implicit system: ActorSystem,
                                      executionContext: ExecutionContextExecutor,
                                      actorMaterializer: ActorMaterializer): Unit = {
    // esTopology will only contain shards that are to be read.
    val shardsToDownload = esTopology.shards.keys.toSeq

    // If we were very clever, we would ensure that we only send one request to a node at a time.
    // Instead, we use a simpler approach and just select shards randomly, and it should be fairly even.
    val shards = mutable.Queue(Random.shuffle(shardsToDownload): _*)

    val shardsInFlight = mutable.Set.empty[Shard]

    val isDone = Promise[Boolean]()

    def launchShard(shard: Shard): Unit = {
      val nodesHostingShard = esTopology.shards(shard)
      val node = nodesHostingShard(Random.nextInt(nodesHostingShard.length)) // pick one node randomly
      val address = esTopology.nodes(node)

      val writer: DataWriter[T] = dataWriterFactory(shard)

      if (shard.downloadAttempt == 0)
        logger.info(s"Starting initial extract for index:${shard.indexName}, shard:${shard.shard}, from:$address.")
      else
        logger.warn(s"Starting extract retry ${shard.downloadAttempt} for index:${shard.indexName}, shard:${shard.shard}, from:$address.")

      infotonsFromShard[T](
        shard = shard,
        httpAddress = address,
        currentOnly = currentOnly,
        extractor = objectExtractor,
        sourceFilter = sourceFilter)
        // TODO: Since the writer will be blocking, should it be dispatched on a separate (bounded) thread pool?
        .toMat(Sink.foreach((infoton: T) => writer.write(infoton)))(Keep.right)
        .run().onComplete {
        case Success(_) =>
          writer.close()
          onDownloadSuccess(shard)

        case Failure(ex) =>
          onDownloadFailure(shard, ex)
      }
    }

    /**
      * Handle a download failure, and retry if we haven't exceeded the retry limit.
      *
      * This handling is specifically designed so that it is safe to handle a failure more than once.
      * The flow in runDownload can fail in two places, and it appeared that in some cases, that a failure in
      * the first part didn't always propagate to the second part. This handling deals deals with either case,
      * and avoids re-launching duplicate downloads.
      */
    def onDownloadFailure(shard: Shard, ex: Throwable): Unit = {

      shards.synchronized {

        if (shardsInFlight.contains(shard)) { // Check if failure was already handled

          shardsInFlight -= shard

          if (shard.downloadAttempt <= maxReadAttempts) {
            logger.warn(s"Failed download for index:${shard.indexName}, shard:${shard.shard}.", ex)

            assert(!shards.contains(shard))
            shards.enqueue(shard.nextAttempt)

            launchShards()
          }
          else {
            logger.warn(s"Failed download for index:${shard.indexName}, shard:${shard.shard}. Giving up!", ex)
            isDone.tryFailure(ex)

            // Don't launch any more shards - let the system wind down to failure
            shards.clear()
          }
        }
        else {
          // This download failure was already handled
        }
      }
    }

    def onDownloadSuccess(shard: Shard): Unit = {
      logger.info(s"Completed download for index:${shard.indexName}, shard:${shard.shard}.")

      shards.synchronized {
        shardsInFlight -= shard

        if (shards.nonEmpty)
          launchShards()
        else if (shardsInFlight.isEmpty)
          isDone.trySuccess(true)
        // else - do nothing - let shards in flight complete
      }
    }

    def launchShards(): Unit = {

      shards.synchronized {

        (1 to (shards.length min (parallelism - shardsInFlight.size))).foreach { _ =>
          val shard = shards.dequeue()
          shardsInFlight += shard
          launchShard(shard)
        }
      }
    }

    launchShards() // Launch the first batch

    if (Await.result(isDone.future, Duration.Inf)) {
      logger.info("Completed successfully.")
    }
  }
}

