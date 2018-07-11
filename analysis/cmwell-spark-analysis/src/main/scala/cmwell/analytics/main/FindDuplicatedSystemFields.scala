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

        val ds = InfotonWithDuplicatedSystemFields()(spark)
          .toDF()
          .repartition(1) // expect a small number, so make the output easier to deal with.
          .cache()

        logger.info(s"There are ${ds.count()} infotons with duplicated system fields.")

        ds.write.csv(Opts.out())
      }
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
