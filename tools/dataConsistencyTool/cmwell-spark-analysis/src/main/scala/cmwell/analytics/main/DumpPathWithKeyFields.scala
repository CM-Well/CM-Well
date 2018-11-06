package cmwell.analytics.main

import cmwell.analytics.data.PathWithKeyFields
import cmwell.analytics.util.CmwellConnector
import cmwell.analytics.util.DatasetFilter
import cmwell.analytics.util.TimestampConversion.timestampConverter
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}


object DumpPathWithKeyFields {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(DumpPathWithKeyFields.getClass)

    // Here, the parallelism defines how many partitions are produced.
    // Having too many partitions (esp. with a shuffle) creates pathological I/O patterns.
    val defaultParallelism = 1 max (Runtime.getRuntime.availableProcessors / 2)

    try {

      object Opts extends ScallopConf(args) {

        val parallelism: ScallopOption[Int] = opt[Int]("parallelism", short = 'p', descr = "The parallelism level", default = Some(defaultParallelism))

        val lastModifiedGteFilter: ScallopOption[java.sql.Timestamp] = opt[java.sql.Timestamp]("lastmodified-gte-filter", descr = "Filter on lastModified >= <value>, where value is an ISO8601 timestamp", default = None)(timestampConverter)
        val pathPrefixFilter: ScallopOption[String] = opt[String]("path-prefix-filter", descr = "Filter on the path prefix matching <value>", default = None)

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
        appName = "Dump path table - key fields",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { spark =>

        val datasetFilter = DatasetFilter(
          lastModifiedGte = Opts.lastModifiedGteFilter.toOption,
          pathPrefix = Opts.pathPrefixFilter.toOption)

        val ds = PathWithKeyFields(Some(datasetFilter))(spark)
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
