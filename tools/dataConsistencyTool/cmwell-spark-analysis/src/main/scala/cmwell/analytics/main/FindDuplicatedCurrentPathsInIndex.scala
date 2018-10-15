package cmwell.analytics.main

import cmwell.analytics.data.{IndexWithSystemFields, Spark}
import cmwell.analytics.util.CmwellConnector
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.joda.time.format.ISODateTimeFormat
import org.rogach.scallop.{ScallopConf, ScallopOption, ValueConverter, singleArgConverter}

import scala.concurrent.duration.Duration

/**
  * When a new version of an infoton is ingested, the index entry for the existing infoton should be marked as
  * current=false, and the index entry for the new version should be marked as current=true. In other words,
  * for each path, there should be exactly one index entry that has current=true.
  *
  * The extract of the index data should use the `--current-only true` option so that the extract is as small
  * as possible, but this will work correctly even if non-current infotons are included in the extract.
  */
object FindDuplicatedCurrentPathsInIndex {

  private val logger = LogManager.getLogger(FindDuplicatedCurrentPathsInIndex.getClass)

  def main(args: Array[String]): Unit = {

    try {

      object Opts extends ScallopConf(args) {

        val durationConverter: ValueConverter[Long] = singleArgConverter[Long](Duration(_).toMillis)

        val index: ScallopOption[String] = opt[String]("index", short = 'x', descr = "The path to the index data (with system fields) in parquet format", required = true)

        val currentThreshold: ScallopOption[Long] = opt[Long]("current-threshold", short = 'c', descr = "Filter out any inconsistencies that are more current than this duration (e.g., 24h)", default = Some(Duration("1d").toMillis))(durationConverter)

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The directory to save the output to (in csv format)", required = true)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Find paths with more than one current=true",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { implicit spark =>

        import spark.implicits._

        // Load the index data (with system fields), and filter out any that are not current.
        val indexRaw = spark.read.parquet(Opts.index()).as[IndexWithSystemFields]
          .filter("current = true")

        // Estimate the ideal number of partitions in the filtered dataset.
        val rowCount = indexRaw.count()
        val rowSize = IndexWithSystemFields.estimateTungstenRowSize(indexRaw)
        val numPartitions = Spark.idealPartitioning(rowSize * rowCount)

        // Repartition into the ideal partition size, partitioning on path so that
        // grouping doesn't need to re-shuffle.
        val index = indexRaw.repartition(numPartitions, indexRaw("path"))

        import org.apache.spark.sql.functions._

        val consistencyThreshold = System.currentTimeMillis - Opts.currentThreshold()

        // Group by path, and keep any groups that have more than one row that are older than the consistency threshold.
        val filtered: DataFrame = index
          .select(
            index("path"),
            // this column counts 1 if it must be consistent, or 0 if it is current and is allowed to be inconsistent.
            when(index("lastModified") < consistencyThreshold, 1).otherwise(0).as("mustBeConsistent"),
            struct("kind", "uuid", "lastModified", "path", "dc", "indexName", "indexTime", "parent", "current").as("data"))
          .groupBy("path")
          .agg(sum("mustBeConsistent").as("mustBeConsistent"), collect_list("data").as("data"))
          .filter("mustBeConsistent > 1")

        // Explode the list of rows in each group
        val positives = filtered
          .select(explode(filtered("data")).as("data"))

        // Reformat the lastModified and indexTime columns as ISO8601.
        // In order to show full precision, a UDF is needed here (Spark functions ignore fractional seconds).
        val formatISO8601 = udf((ts: Long) => ISODateTimeFormat.dateTime.print(ts))

        // Un-struct the data field so we have the original index data again, and write it out.
        positives
          .select(
            positives("data.kind").as("kind"),
            positives("data.uuid").as("uuid"),
            formatISO8601(positives("data.lastModified")).as("lastModified"),
            positives("data.path").as("path"),
            positives("data.dc").as("dc"),
            positives("data.indexName").as("indexName"),
            formatISO8601(positives("data.indexTime")).as("indexTime"),
            positives("data.parent").as("parent"),
            positives("data.current").as("current")
          )
          .filter("not substring(path, 0, 6) = '/meta/'") // Not interested in these (for now).
          .write.csv(Opts.out())
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
