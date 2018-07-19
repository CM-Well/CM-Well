package cmwell.analytics.main

import cmwell.analytics.util.CmwellConnector
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.annotation.tailrec

case class UuidAndRdfType(uuid: String, rdfType: String)

/**
  * Extract a dataset of { uuid, rdfType } for all infotons.
  */
object DumpInfotonWithUuidAndRdfType {

  private val logger = LogManager.getLogger(DumpInfotonWithUuidAndRdfType.getClass)

  def main(args: Array[String]): Unit = {

    try {

      object Opts extends ScallopConf(args) {

        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The file to save the output to", required = true)
        val shell: ScallopOption[Boolean] = opt[Boolean]("spark-shell", short = 's', descr = "Run a Spark shell", required = false, default = Some(false))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        val format: ScallopOption[String] = opt[String]("format", short = 'f', descr = "The output format: csv | parquet", required = false, default = Some("parquet"))

        validateOpt(format) {
          case Some("parquet") | Some("csv") => Right(Unit)
          case _ => Left(s"Invalid format - must be 'csv' or 'parquet'.")
        }

        verify()
      }

      CmwellConnector(
        cmwellUrl = Opts.url(),
        appName = "Extract all rows from infoton with uuid, rdfType",
        sparkShell = Opts.shell()
      ).withSparkSessionDo { implicit spark =>

        // This code fragment was extracting all the hashes for ontology namespaces, so it could be validated below.

        //        val esContactPoint = FindContactPoints.es(Opts.url())
        //
        //        val query = """{"query":{"bool":{"must":[{"term":{"system.parent":"/meta/ns"}}]}}}"""
        //        val json = HttpUtil.getJson("http://" + esContactPoint + "/cm_well_all/_search", query, "text/plain")
        //        val hits = json.findValue("hits").findValue("hits")
        //
        //        val hashes = hits.iterator.asScala.map { hit =>
        //          val path = hit.findValue("_source").findValue("system").findValue("path").asText
        //          path.substring(path.lastIndexOf("/") + 1)
        //        }.toSet

        import com.datastax.spark.connector._
        val rdd = spark.sparkContext.cassandraTable("data2", "infoton")
          .spanBy(row => row.getString("uuid"))
          .map { case (uuid, fields) =>

            val it = fields.iterator

            @tailrec
            def findRdfType(): String = {
              if (it.hasNext) {
                val row = it.next()
                val field = row.getString("field")

                if (field.startsWith("r$type.") /* && hashes.contains(field.substring("r$type.".length)) */ ) {
                  row.getString("value")
                }
                else {
                  findRdfType()
                }
              }
              else {
                null
              }
            }

            UuidAndRdfType(uuid, findRdfType())
          }

        val ds = spark.createDataFrame(rdd)

        Opts.format() match {
          case "parquet" => ds.write.parquet(Opts.out())
          case "csv" => ds.write.csv(Opts.out())
        }
      }
    }

    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
  }
}
