package cmwell.analytics.main

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cmwell.analytics.data.{DataWriterFactory, IndexWithKeyFields}
import cmwell.analytics.downloader.PartitionedDownloader
import cmwell.analytics.util.TimestampConversion.timestampConverter
import cmwell.analytics.util.{DiscoverEsTopology, FindContactPoints}
import org.apache.commons.io.FileUtils
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.concurrent.ExecutionContextExecutor

object DumpKeyFieldsFromEs {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(DumpKeyFieldsFromEs.getClass)

    implicit val system: ActorSystem = ActorSystem("dump-key-fields-from-es")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    try {
      // Since we expect this to be run on a CM-Well node, the default parallelism is to use half the processors
      // so as to avoid starving the CM-Well node from processor resources. A higher level of parallelism might
      // be possible (without interfering with CM-Well) since most of the work will actually be on the ES side.
      val defaultParallelism = 1 max (Runtime.getRuntime.availableProcessors / 2)

      object Opts extends ScallopConf(args) {

        val readIndex: ScallopOption[String] = opt[String]("read-index", short = 'i', descr = "The name of the index to read from (default: cm_well_all)", required = false)
        val parallelism: ScallopOption[Int] = opt[Int]("parallelism", short = 'p', descr = "The parallelism level", default = Some(defaultParallelism))

        val currentFilter: ScallopOption[Boolean] = opt[Boolean]("current-filter", short = 'c', descr = "Filter on current status", default = None)
        val lastModifiedGteFilter: ScallopOption[java.sql.Timestamp] = opt[java.sql.Timestamp]("lastmodified-gte-filter", descr = "Filter on lastModified >= <value>, where value is an ISO8601 timestamp", default = None)(timestampConverter)
        val pathPrefixFilter: ScallopOption[String] = opt[String]("path-prefix-filter", descr = "Filter on the path prefix matching <value>", default = None)

        val format: ScallopOption[String] = opt[String]("format", short = 'f', descr = "The data format: either 'parquet' or 'csv'", default = Some("parquet"))
        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to", required = true)

        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        val sourceFilter: ScallopOption[Boolean] = toggle("source-filter", noshort = true, default = Some(true), prefix = "no-",
          descrNo = "Do not filter _source fields (workaround for bad index)", descrYes = "Use source filtering to reduce network traffic")

        verify()
      }

      val esContactPoint = FindContactPoints.es(Opts.url())
      val indexesOrAliasesToRead = Opts.readIndex.toOption.fold(Seq("cm_well_all"))(Seq(_))
      val esTopology = DiscoverEsTopology(esContactPoint = esContactPoint, aliases = indexesOrAliasesToRead)

      // Calling script should clear output directory as necessary.

      val objectExtractor = IndexWithKeyFields
      val dataWriterFactory = DataWriterFactory.file(format = Opts.format(), objectExtractor, outDirectory = Opts.out())

      PartitionedDownloader.runDownload(
        esTopology = esTopology,
        parallelism = Opts.parallelism(),

        currentFilter = Opts.currentFilter.toOption,
        lastModifiedGteFilter = Opts.lastModifiedGteFilter.toOption,
        pathPrefixFilter = Opts.pathPrefixFilter.toOption,

        objectExtractor = objectExtractor,
        dataWriterFactory = dataWriterFactory,
        sourceFilter = Opts.sourceFilter())

      // The Hadoop convention is to touch the (empty) _SUCCESS file to signal successful completion.
      FileUtils.touch(Paths.get(Opts.out(), "_SUCCESS").toFile)
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
