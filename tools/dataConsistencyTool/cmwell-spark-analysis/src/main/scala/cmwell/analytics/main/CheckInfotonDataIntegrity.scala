package cmwell.analytics.main

import cmwell.analytics.data.InfotonDataIntegrity
import cmwell.analytics.util.{CmwellConnector, DatasetFilter}
import cmwell.analytics.util.TimestampConversion.timestampConverter
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

object CheckInfotonDataIntegrity {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(CheckInfotonDataIntegrity.getClass)

    try {

      object Opts extends ScallopConf(args) {

        val lastModifiedGteFilter: ScallopOption[java.sql.Timestamp] = opt[java.sql.Timestamp]("lastmodified-gte-filter", descr = "Filter on lastModified >= <value>, where value is an ISO8601 timestamp", default = None)(timestampConverter)
        val pathPrefixFilter: ScallopOption[String] = opt[String]("path-prefix-filter", descr = "Filter on the path prefix matching <value>", default = None)

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to", required = true)
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Check infoton data integrity"
      ).withSparkSessionDo { spark =>

        val datasetFilter = DatasetFilter(
          lastModifiedGte = Opts.lastModifiedGteFilter.toOption,
          pathPrefix = Opts.pathPrefixFilter.toOption)

        val ds = InfotonDataIntegrity(Some(datasetFilter))(spark)

        val damagedInfotons = ds.filter(infoton =>
          infoton.hasIncorrectUuid ||
            infoton.hasDuplicatedSystemFields ||
            infoton.hasInvalidContent ||
            infoton.hasMissingOrIllFormedSystemFields
        )

        damagedInfotons.select("uuid", "lastModified", "path",
          "hasIncorrectUuid", "hasMissingOrIllFormedSystemFields", "hasDuplicatedSystemFields", "hasInvalidContent", "hasUnknownSystemField")
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