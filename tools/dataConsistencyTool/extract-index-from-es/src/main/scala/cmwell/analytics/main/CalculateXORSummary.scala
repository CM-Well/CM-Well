package cmwell.analytics.main

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cmwell.analytics.data.{IndexWithSourceHash, XORSummary, XORSummaryFactory}
import cmwell.analytics.downloader.PartitionedDownloader
import cmwell.analytics.util.{DiscoverEsTopology, FindContactPoints}
import org.apache.commons.codec.binary.Hex
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.concurrent.ExecutionContextExecutor

object CalculateXORSummary {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(CalculateXORSummary.getClass)

    // Since we expect this to be run on a CM-Well node, the default parallelism is to use half the processors
    // so as to avoid starving the CM-Well node from processor resources. A higher level of parallelism might
    // be possible (without interfering with CM-Well) since most of the work will actually be on the ES side.
    val defaultParallelism = 1 max (Runtime.getRuntime.availableProcessors / 2)

    implicit val system: ActorSystem = ActorSystem("xor-summary")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    try {

      object Opts extends ScallopConf(args) {

        val readIndex: ScallopOption[String] = opt[String]("read-index", short = 'i', descr = "The name of the index to read from", required = false)

        val parallelism: ScallopOption[Int] = opt[Int]("parallelism", short = 'p', descr = "The parallelism level", default = Some(defaultParallelism))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      val esContactPoint = FindContactPoints.es(Opts.url())
      val indexesOrAliasesToRead = Opts.readIndex.toOption.fold(Seq("cm_well_all"))(Seq(_))
      val esTopology = DiscoverEsTopology(esContactPoint = esContactPoint, aliases = indexesOrAliasesToRead)

      val dataWriterFactory = new XORSummaryFactory()

      PartitionedDownloader.runDownload(
        esTopology = esTopology,
        parallelism = Opts.parallelism(),
        objectExtractor = IndexWithSourceHash,
        dataWriterFactory = dataWriterFactory.apply,
        sourceFilter = false)

      // Summarize the summaries down to the index level.
      val summaryByIndex: Map[String, XORSummary] = dataWriterFactory.shardSummaries
        .groupBy { case (shard, _) => shard.indexName }
        .map { case (indexName, summaryMap) => indexName -> summaryMap.values.reduce(XORSummary.combine) }

      // TODO: Fix questionable JSON generation
      val r = "{" +
        summaryByIndex.map { case (index, summary) =>
          val x = Hex.encodeHexString(summary.summary)
          s""" { "index": "$index", "summary": "$x" } """
        }.mkString("\n") + "}"

      println(r)
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
