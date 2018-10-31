package cmwell.analytics.main

import cmwell.analytics.data.Spark
import cmwell.analytics.util.ConsistencyThreshold.defaultConsistencyThreshold
import cmwell.analytics.util.ISO8601.{instantToMillis, instantToText}
import cmwell.analytics.util.TimestampConversion.timestampConverter
import cmwell.analytics.util.{CmwellConnector, DatasetFilter, KeyFields, SetDifferenceAndFilter}
import org.apache.log4j.LogManager
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.udf
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.{ScallopConf, ScallopOption, ValueConverter, singleArgConverter}

/**
  * This analysis compares the uuids in the infoton and paths tables (from Cassandra) and the index (from ES),
  * and is intended to the internal consistency of uuids within a single CM-Well instance.
  * The set difference is calculated between each source (infoton, path, index) and in each direction.
  *
  * The result is filtered to exclude false positives, which are current infotons
  * (i.e., lastModified > now - consistencyThreshold) that have not reached consistency yet.
  *
  * The results are written as CSV files.
  */
object SetDifferenceUuids {

  private val logger = LogManager.getLogger(SetDifferenceUuids.getClass)

  // Here, the parallelism defines how many partitions are produced.
  // Having too many partitions (esp. with a shuffle) creates pathological I/O patterns.
  private val defaultParallelism = 1 max (Runtime.getRuntime.availableProcessors / 2)

  def main(args: Array[String]): Unit = {

    try {

      object Opts extends ScallopConf(args) {

        private val instantConverter: ValueConverter[Long] = singleArgConverter[Long](instantToMillis)

        val parallelism: ScallopOption[Int] = opt[Int]("parallelism", short = 'p', descr = "The parallelism level", default = Some(defaultParallelism))

        val lastModifiedGteFilter: ScallopOption[java.sql.Timestamp] = opt[java.sql.Timestamp]("lastmodified-gte-filter", descr = "Filter on lastModified >= <value>, where value is an ISO8601 timestamp", default = None)(timestampConverter)
        val pathPrefixFilter: ScallopOption[String] = opt[String]("path-prefix-filter", descr = "Filter on the path prefix matching <value>", default = None)

        val infoton: ScallopOption[String] = opt[String]("infoton", short = 'i', descr = "The path to the infoton {uuid,lastModified,path} in parquet format", required = true)
        val index: ScallopOption[String] = opt[String]("index", short = 'x', descr = "The path to the index {uuid,lastModified,path} in parquet format", required = true)
        val path: ScallopOption[String] = opt[String]("path", short = 'h', descr = "The path to the path {uuid,lastModified,path} in parquet format", required = true)

        val consistencyThreshold: ScallopOption[Long] = opt[Long]("consistency-threshold", short = 'c', descr = "Ignore any inconsistencies at or after this instant", default = Some(defaultConsistencyThreshold))(instantConverter)

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The directory to save the output to (in csv format)", required = true)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Set Difference UUIDs infoton/path/index",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { implicit spark =>


        logger.info(s"Using a consistency threshold of ${instantToText(Opts.consistencyThreshold())}.")
        val datasetFilter = DatasetFilter(
          lastModifiedGte = Opts.lastModifiedGteFilter.toOption,
          pathPrefix = Opts.pathPrefixFilter.toOption)

        import spark.implicits._

        // Since we will be doing multiple set differences with the same files, do an initial repartition and cache to
        // avoid repeating shuffles. We also want to calculate an ideal partition size to avoid OOM.

        def load(name: String): Dataset[KeyFields] = {
          val ds = spark.read.parquet(name)
            .as[KeyFields]

          datasetFilter.applyFilter(ds, forAnalysis = true)
        }

        // The extract from ES might contain system fields, so we want to avoid having to read those extra fields
        // from Parquet (so make sure we don't use an RDD scan).
        // The lastModified is stored as a Long (as opposed to java.sql.Timestamp used in KeyFields), so we need
        // a simple UDF to convert (stupid conversion - just the type is changing).
        // TODO: Change the ES utility to use java.sql.Timestamp as the type for lastModified (and indexTime).
        def loadES(name: String): Dataset[KeyFields] = {

          val convertLongToTimestamp: Long => java.sql.Timestamp = new java.sql.Timestamp(_)
          val convertLongToTimestampUdf = udf(convertLongToTimestamp)

          val ds = spark.read.parquet(name)

          val dsWithLastModifiedConverted = ds
            .select(ds("uuid"), convertLongToTimestampUdf(ds("lastModified")).as("lastModified"), ds("path"))
            .as[KeyFields]

          datasetFilter.applyFilter(dsWithLastModifiedConverted, forAnalysis = true)
        }

        val infotonRaw = load(Opts.infoton())
        val count = infotonRaw.count()
        val rowSize = KeyFields.estimateTungstenRowSize(infotonRaw)
        val numPartitions = Spark.idealPartitioning(rowSize * count * 2)

        def repartition(ds: Dataset[KeyFields]): Dataset[KeyFields] =
          ds.repartition(numPartitions, ds("uuid")).persist(StorageLevel.DISK_ONLY)

        val infoton = repartition(infotonRaw)
        // Having too many input partitions produces extremely fragmented shuffle output.
        // The C* sources will already have been coalesced to a smaller number of partitions, but the
        // ES source doesn't yet support that, so we explicitly coalesce before shuffling.
        val index = repartition(loadES(Opts.index()).coalesce(Opts.parallelism() * CmwellConnector.coalesceParallelismMultiplier))
        val path = repartition(load(Opts.path()))

        SetDifferenceAndFilter(infoton, path, Opts.consistencyThreshold())
          .write.csv(Opts.out() + "/infoton-except-path")

        SetDifferenceAndFilter(infoton, index, Opts.consistencyThreshold())
          .write.csv(Opts.out() + "/infoton-except-index")

        SetDifferenceAndFilter(path, index, Opts.consistencyThreshold())
          .write.csv(Opts.out() + "/path-except-index")

        SetDifferenceAndFilter(path, infoton, Opts.consistencyThreshold())
          .write.csv(Opts.out() + "/path-except-infoton")

        SetDifferenceAndFilter(index, infoton, Opts.consistencyThreshold())
          .write.csv(Opts.out() + "/index-except-infoton")

        SetDifferenceAndFilter(index, path, Opts.consistencyThreshold())
          .write.csv(Opts.out() + "/index-except-path")
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
