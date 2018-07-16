package cmwell.analytics.main

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import cmwell.analytics.data.InfotonAndIndexWithSystemFields
import cmwell.analytics.util.Connector
import org.apache.commons.io.FileUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.breakOut

object AnalyzeInconsistenciesResult {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(AnalyzeInconsistenciesResult.getClass)

    try {

      object Opts extends ScallopConf(args) {

        val in: ScallopOption[String] = opt[String]("in", short = 'i', descr = "The path to read the (parquet) inconsistencies dataset from", required = true)
        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the (csv) output to", required = true)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))

        verify()
      }

      Connector(
        appName = "Analyze InfotonAndIndexWithSystemFields Output",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { spark =>

        val ds: DataFrame = spark.read.parquet(Opts.in())

        import org.apache.spark.sql.functions._

        // A column expression that counts the number of failures for each constraint.
        // This will also include null counts, needed to interpret the results.
        val constraints: Seq[(String, Column)] = InfotonAndIndexWithSystemFields.constraints(ds).map { case (name, predicate) =>
          name -> sum(when(predicate, 0L).otherwise(1L)).as(name)
        }(breakOut)

        // Compute the failure counts
        val failureCounts: Row = ds.agg(constraints.head._2, constraints.tail.map(_._2): _*).head

        val results = for {
          i <- constraints.indices
          constraintName = constraints(i)._1
          failureCount = if (failureCounts.isNullAt(i)) 0 else failureCounts.getAs[Long](i)
        } yield s"$constraintName,$failureCount"

        FileUtils.write(new File(Opts.out()), "constraint,failures\n" + results.mkString("\n"), UTF_8)
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
