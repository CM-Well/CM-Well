package cmwell.analytics.main

import cmwell.analytics.data.InfotonWithKeyFields
import cmwell.analytics.util.{CmwellConnector, DatasetFilter, KeyFields}
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

case class ParentPathRow(parentPath: String)

object FindMissingPaths {

  /**
    * Find missing paths using the contents of the infoton table.
    * This will only work correctly if the data is internally consistent.
    *
    * This includes the ability to use a path prefix filter, but doing could cause
    * some parent to show up as missing. That can easily be verified manually.
    */
  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(FindMissingPaths.getClass)

    try {

      object Opts extends ScallopConf(args) {

        val pathPrefixFilter: ScallopOption[String] = opt[String]("path-prefix-filter", descr = "Filter on the path prefix matching <value>", default = None)

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to", required = true)
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Find missing paths"
      ).withSparkSessionDo { spark: SparkSession =>

        val datasetFilter = DatasetFilter(pathPrefix = Opts.pathPrefixFilter.toOption)

        val ds = InfotonWithKeyFields(Some(datasetFilter))(spark).cache()

        import spark.implicits._
        // Find all the parent paths that should exist

        val pathsThatShouldExist = ds.rdd.map { keyFields: KeyFields =>
          val path = keyFields.path

          // ASSUME: The path is normalized and doesn't end in a slash.
          ParentPathRow(path.substring(0, path.lastIndexOf("/")))

        }.filter(_.parentPath.nonEmpty) // Don't care about root
          .toDS()
          .distinct()

        // Find which pathsThatShouldExist, but don't actually existing in ds.path

        val missingPaths = pathsThatShouldExist.join(ds, ds("path") === pathsThatShouldExist("parentPath"), "leftanti")

        missingPaths.write.csv(Opts.out())
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
