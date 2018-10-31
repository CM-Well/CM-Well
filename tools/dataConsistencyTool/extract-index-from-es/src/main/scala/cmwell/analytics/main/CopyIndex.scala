package cmwell.analytics.main

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cmwell.analytics.data.{DataWriterFactory, IndexWithCompleteDocument}
import cmwell.analytics.downloader.PartitionedDownloader
import cmwell.analytics.util.{DiscoverEsTopology, FindContactPoints}
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.concurrent.ExecutionContextExecutor

object CopyIndex {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(CopyIndex.getClass)

    // Since we expect this to be run on a CM-Well node, the default parallelism is to use half the processors
    // so as to avoid starving the CM-Well node from processor resources. A higher level of parallelism might
    // be possible (without interfering with CM-Well) since most of the work will actually be on the ES side.
    val defaultParallelism = 1 max (Runtime.getRuntime.availableProcessors / 2)

    implicit val system: ActorSystem = ActorSystem("copy-index")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    try {

      object Opts extends ScallopConf(args) {

        val readIndex: ScallopOption[String] = opt[String]("read-index", short = 'i', descr = "The name of the index to read from", required = true)
        val writeIndex: ScallopOption[String] = opt[String]("write-index", short = 'w', descr = "The name of the index to write to", required = true)

        val parallelism: ScallopOption[Int] = opt[Int]("parallelism", short = 'p', descr = "The parallelism level", default = Some(defaultParallelism))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      val esContactPoint = FindContactPoints.es(Opts.url())
      val indexesOrAliasesToRead = Opts.readIndex.toOption.fold(Seq("cm_well_all"))(Seq(_))
      val esTopology = DiscoverEsTopology(esContactPoint = esContactPoint, aliases = indexesOrAliasesToRead)

      val dataWriterFactory = DataWriterFactory.index[IndexWithCompleteDocument](
        indexName = Opts.writeIndex(),
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
