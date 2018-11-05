package cmwell.analytics.main

import cmwell.analytics.data.InfotonAndIndexWithSystemFields
import cmwell.analytics.data.InfotonAndIndexWithSystemFields.{isConsistent, isWellFormed}
import cmwell.analytics.util.CmwellConnector
import cmwell.analytics.util.ConsistencyThreshold.defaultConsistencyThreshold
import cmwell.analytics.util.ISO8601.{instantToMillis, instantToText}
import org.apache.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.joda.time.format.ISODateTimeFormat
import org.rogach.scallop.{ScallopConf, ScallopOption, ValueConverter, singleArgConverter}

import scala.util.control.NonFatal

object FindInfotonIndexInconsistencies {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(FindInfotonIndexInconsistencies.getClass)

    try {

      object Opts extends ScallopConf(args) {

        private val instantConverter: ValueConverter[Long] = singleArgConverter[Long](instantToMillis)

        // If this parameter is not supplied, the (unreliable) ES Spark connector is used to extract the data from the es index.
        val esExtract: ScallopOption[String] = opt[String]("es", short = 'e', descr = "The path where the (parquet) extract of system fields the es index are stored", required = false)

        val consistencyThreshold: ScallopOption[Long] = opt[Long]("consistency-threshold", short = 'c', descr = "Ignore any inconsistencies at or after this instant", default = Some(defaultConsistencyThreshold))(instantConverter)

        val outParquet: ScallopOption[String] = opt[String]("out-parquet", short = 'p', descr = "The path to save the output to (in parquet format)", required = false)
        val outCsv: ScallopOption[String] = opt[String]("out-csv", short = 'v', descr = "The path to save the output to (in CSV format)", required = false)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Find inconsistencies between system fields in Infoton and Index",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { spark =>

        logger.info(s"Using a consistency threshold of ${instantToText(Opts.consistencyThreshold())}.")

        val ds = InfotonAndIndexWithSystemFields(esExtractPath = Opts.esExtract.toOption)(spark)

        // Filter out any inconsistencies found if more current than this point in time.
        val i = ds.schema.indexWhere(_.name == "infoton_lastModified")
        val filterCurrent: Row => Boolean = { row: Row =>

          val parser = ISODateTimeFormat.dateTimeParser
          if (row.isNullAt(i))
            true // Shouldn't be null, but don't filter out if we can't get a lastModified
          else
            try {
              parser.parseMillis(row.getAs[String](i)) < Opts.consistencyThreshold()
            }
            catch {
              case NonFatal(_) => true // Don't filter out if lastModified couldn't be converted
            }
        }

        val inconsistentData = ds.filter(not(isConsistent(ds) && isWellFormed(ds)))
          .filter(filterCurrent)
          .cache()

        // Save the inconsistent data in Parquet format suitable for additional analysis
        if (Opts.outParquet.isDefined)
          inconsistentData
            .write
            .parquet(Opts.outParquet())

        // Save the inconsistent data to a single CSV file suitable for reporting.
        if (Opts.outCsv.isDefined)
          inconsistentData
            .coalesce(1)
            .write
            .option("header", value = true)
            .csv(Opts.outCsv())
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
