package cmwell.analytics.main

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cmwell.analytics.data.{DataWriterFactory, IndexWithCompleteDocument}
import cmwell.analytics.downloader.PartitionedDownloader
import cmwell.analytics.util.{DiscoverEsTopology, FindContactPoints}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

object CopyIndexesWithMapping {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(CopyIndexesWithMapping.getClass)

    // Since we expect this to be run on a CM-Well node, the default parallelism is to use half the processors
    // so as to avoid starving the CM-Well node from processor resources. A higher level of parallelism might
    // be possible (without interfering with CM-Well) since most of the work will actually be on the ES side.
    val defaultParallelism = 1 max (Runtime.getRuntime.availableProcessors / 2)

    implicit val system: ActorSystem = ActorSystem("copy-index-with-mapping")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    try {

      object Opts extends ScallopConf(args) {

        val indexMap: ScallopOption[String] = opt[String]("index-map", short = 'i', descr = "A map from source to target index names, in JSON format", required = true)

        val parallelism: ScallopOption[Int] = opt[Int]("parallelism", short = 'p', descr = "The parallelism level", default = Some(defaultParallelism))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      val esContactPoint = FindContactPoints.es(Opts.url())

      // Expect a map in the form: { "sourceIndex1": "targetIndex1", "sourceIndex2": "targetIndex2", ... }
      val indexMap: Map[String, String] = new ObjectMapper().readTree(Opts.indexMap()).fields.asScala.map { entry =>
        entry.getKey -> entry.getValue.asText
      }.toMap

      val esTopology = DiscoverEsTopology(esContactPoint = esContactPoint, aliases = indexMap.keys.toSeq)

      // Validate that the index-map parameter specified valid index names, and not aliases.
      for (indexName <- indexMap.keys)
        if (!esTopology.allIndexNames.contains(indexName))
          throw new RuntimeException(s"index-map parameter included $indexName as a source, which is not a valid index name.")

      for (indexName <- indexMap.values)
        if (!esTopology.allIndexNames.contains(indexName))
          throw new RuntimeException(s"index-map parameter included $indexName as a target, which is not a valid index name.")

      val dataWriterFactory = DataWriterFactory.index[IndexWithCompleteDocument](
        indexMap = indexMap,
        esEndpoint = esContactPoint)

      PartitionedDownloader.runDownload(
        esTopology = esTopology,
        parallelism = Opts.parallelism(),
        objectExtractor = IndexWithCompleteDocument,
        dataWriterFactory = dataWriterFactory,
        sourceFilter = false)
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
    finally {
      system.terminate()
    }
  }
}
