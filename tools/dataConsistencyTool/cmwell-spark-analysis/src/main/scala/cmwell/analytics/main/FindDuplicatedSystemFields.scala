package cmwell.analytics.main

import cmwell.analytics.data.InfotonWithDuplicatedSystemFields
import cmwell.analytics.util.CmwellConnector
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

object FindDuplicatedSystemFields {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(FindDuplicatedSystemFields.getClass)

    try {

      object Opts extends ScallopConf(args) {

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to", required = true)
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Find infotons with duplicated system fields"
      ).withSparkSessionDo { spark =>

        import spark.implicits._

        InfotonWithDuplicatedSystemFields()(spark)
          .toDF
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
