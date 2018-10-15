package cmwell.analytics.main

import cmwell.analytics.data.IndexWithSystemFields
import cmwell.analytics.util.CmwellConnector
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
  * This doesn't work very well due to issues with the ES Spark connector.
  * Use the separate extract-index-from-es project instead.
  */
object DumpIndexWithSystemFields {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(DumpIndexWithSystemFields.getClass)

    // Here, the parallelism defines how many partitions are produced.
    // Having too many partitions (esp. with a shuffle) creates pathological I/O patterns.
    val defaultParallelism = 1 max (Runtime.getRuntime.availableProcessors / 2)

    try {

      object Opts extends ScallopConf(args) {

        val parallelism: ScallopOption[Int] = opt[Int]("parallelism", short = 'p', descr = "The parallelism level", default = Some(defaultParallelism))

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to", required = true)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        val format: ScallopOption[String] = opt[String]("format", short = 'f', descr = "The output format: csv | parquet", required = false, default = Some("parquet"))

        validateOpt(format) {
          case Some("parquet") | Some("csv") => Right(Unit)
          case _ => Left(s"Invalid format - must be 'csv' or 'parquet'.")
        }

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Dump system fields from Elasticsearch indexes",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { spark =>

        val ds = IndexWithSystemFields()(spark)
          .coalesce(Opts.parallelism() * CmwellConnector.coalesceParallelismMultiplier)

        Opts.format() match {
          case "parquet" => ds.write.parquet(Opts.out())
          case "csv" => ds.write.csv(Opts.out())
        }

      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
