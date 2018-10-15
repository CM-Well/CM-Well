package cmwell.analytics.data

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import cmwell.analytics.util.Connector
import cmwell.analytics.util.StringUtil._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

object ExtractFromParquet {

  def main(args: Array[String]): Unit = {

    object Opts extends ScallopConf(args) {

      val pathsToFind: ScallopOption[String] = opt[String]("paths-to-find", short = 'f', descr = "A file containing the list of paths to look for", required = true)
      val parquetData: ScallopOption[String] = opt[String]("parquet-file", short = 'p', descr = "A Parquet file containing the data; single string column rdfStatement", required = true)
      val extractedData: ScallopOption[String] = opt[String]("extracted-data", short = 'd', descr = "The file that extracted data will be written to (in nquads format)", required = true)
      val pathsNotFound: ScallopOption[String] = opt[String]("paths-not-found", short = 'n', descr = "The output file that any paths that were not found are written to", required = true)
      val pathsFound: ScallopOption[String] = opt[String]("paths-found", short = 'a', descr = "The output file containing the paths that we found are written to", required = true)

      verify()
    }

    Connector(sparkShell = true, appName = "Extract from parquet").withSparkSessionDo {
      spark: SparkSession =>

        val pathsToFind = Set(splitLines(FileUtils.readFileToString(new File(Opts.pathsToFind()), UTF_8)): _*)

        val ds: DataFrame = spark.read.parquet(Opts.parquetData())

        // Cheesy parsing of path from an RDF nquad, but sufficient for this purpose
        def extractPath(rdfStatement: String): String = rdfStatement.substring(7, rdfStatement.indexOf(">"))

        val statementsFound = ds.rdd.filter { row: Row =>

          val statement = row.getAs[String]("rdfStatement")
          val path = extractPath(statement)

          pathsToFind.contains(path)
        }.collect() // expect the result to be small, so collect is OK

        // Save all the paths that were not found to file - look for them in other files.
        val pathsFound: Set[String] = Set(statementsFound.map(row => extractPath(row.getString(0))): _*)
        println(s"There were ${pathsFound.size} paths found (out of ${pathsToFind.size}).")
        FileUtils.writeStringToFile(new File(Opts.pathsFound()), pathsFound.mkString("\n"), UTF_8, false)

        val pathsNotFound = pathsToFind.diff(pathsFound)
        println(s"There were ${pathsNotFound.size} paths not found.")
        FileUtils.writeStringToFile(new File(Opts.pathsNotFound()), pathsNotFound.mkString("\n"), UTF_8, false)

        // Save the RDF statements for the paths that were found
        val x = statementsFound.map(row => row.getString(0)).mkString("\n")
        FileUtils.writeStringToFile(new File(Opts.extractedData()), x, UTF_8, false)
    }
  }
}
