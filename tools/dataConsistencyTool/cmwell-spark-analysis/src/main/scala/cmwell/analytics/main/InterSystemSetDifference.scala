package cmwell.analytics.main

import cmwell.analytics.data.Spark
import cmwell.analytics.util._
import org.apache.log4j.LogManager
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.{ScallopConf, ScallopOption, ValueConverter, singleArgConverter}

import scala.concurrent.duration.Duration

/**
  * This analysis compares the uuids between two CM-Well sites.
  * Typically, the extracts of key fields from the infoton table that were extracted for the internal consistency
  * analysis run are re-used here.
  *
  * The two sites are referred to here as site1 and site2, but the caller should provide meaningful names for them,
  * otherwise it is hard to understand the results.
  *
  * The set difference between the two systems are calculated in both directions (site1 - site2 and
  * site2 - site1).
  *
  * The result is filtered to exclude false positives, which are current infotons
  * (i.e., lastModified > now - currentThreshold) that have not reached consistency yet.
  *
  * The results are written as CSV files. There will be a single CSV file within each result.
  */
object InterSystemSetDifference {

  private val logger = LogManager.getLogger(SetDifferenceUuids.getClass)

  def main(args: Array[String]): Unit = {

    try {

      object Opts extends ScallopConf(args) {

        val durationConverter: ValueConverter[Long] = singleArgConverter[Long](Duration(_).toMillis)

        val lastModifiedGteFilter: ScallopOption[java.sql.Timestamp] = opt[java.sql.Timestamp]("lastmodified-gte-filter", descr = "Filter on lastModified >= <value>, where value is an ISO8601 timestamp", default = None)(timestampConverter)
        val pathPrefixFilter: ScallopOption[String] = opt[String]("path-prefix-filter", descr = "Filter on the path prefix matching <value>", default = None)

        val site1Data: ScallopOption[String] = opt[String]("site1data", short = '1', descr = "The path to the site1 data {uuid,lastModified,path} in parquet format", required = true)
        val site2Data: ScallopOption[String] = opt[String]("site2data", short = '2', descr = "The path to the site2 data {uuid,lastModified,path} in parquet format", required = true)

        val site1Name: ScallopOption[String] = opt[String]("site1name", descr = "The logical name for site1", default = Some("site1"))
        val site2Name: ScallopOption[String] = opt[String]("site2name", descr = "The logical name for site2", default = Some("site2"))

        val currentThreshold: ScallopOption[Long] = opt[Long]("current-threshold", short = 'c', descr = "Filter out any inconsistencies that are more current than this duration (e.g., 24h)", default = Some(Duration("1d").toMillis))(durationConverter)

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The directory to save the output to (in csv format)", required = true)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))

        verify()
      }

      Connector(
        appName = "Compare UUIDs between CM-Well instances",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { implicit spark =>

        val datasetFilter = DatasetFilter(
          lastModifiedGte = Opts.lastModifiedGteFilter.toOption,
          pathPrefix = Opts.pathPrefixFilter.toOption)

        // Since we will be doing multiple set differences with the same files, do an initial repartition and cache to
        // avoid repeating shuffles. We also want to calculate an ideal partition size to avoid OOM.

        def load(name: String): Dataset[KeyFields] = {
          import spark.implicits._
          val ds = spark.read.parquet(name).as[KeyFields]

          datasetFilter.applyFilter(ds, forAnalysis = true)
        }

        val site1Raw = load(Opts.site1Data())
        val count = site1Raw.count()
        val rowSize = KeyFields.estimateTungstenRowSize(site1Raw)
        val numPartitions = Spark.idealPartitioning(rowSize * count * 2)

        def repartition(ds: Dataset[KeyFields]): Dataset[KeyFields] =
          ds.repartition(numPartitions, ds("uuid")).persist(StorageLevel.DISK_ONLY)

        val site1 = repartition(site1Raw)
        val site2 = repartition(load(Opts.site2Data()))

        SetDifferenceAndFilter(site1, site2, Opts.currentThreshold(), filterOutMeta = true)
          .write.csv(Opts.out() + s"/${Opts.site1Name()}-except-${Opts.site2Name()}")

        SetDifferenceAndFilter(site2, site1, Opts.currentThreshold(), filterOutMeta = true)
          .write.csv(Opts.out() + s"/${Opts.site2Name()}-except-${Opts.site1Name()}")
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
