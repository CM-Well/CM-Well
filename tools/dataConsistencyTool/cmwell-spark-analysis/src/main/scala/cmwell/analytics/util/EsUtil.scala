package cmwell.analytics.util

import cmwell.analytics.data.IndexWithUuidsOnly
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object EsUtil {

  /** Count all the documents in the cm_well_all alias. */
  def countCmWellAllDocuments(currentOnly: Boolean = false)(implicit spark: SparkSession): Long = {

    // Get the ES contact point out of the Spark context
    val host = spark.conf.get("es.nodes")
    val port = spark.conf.get("es.port")

    val query = IndexWithUuidsOnly.queryWithoutFields(currentOnly = currentOnly)
    val json = HttpUtil.getJson(s"http://$host:$port/cm_well_all/_count", query, "text/plain")

    json.get("count").asLong // TODO: No error checking on document
  }

  def nsPrefixes(httpAddress: String): Map[String, String] = {

    val limit = 10000 // max # hits that ES will allow in a single response
    val query = s"""{"query":{"bool":{"must":[{"term":{"system.parent.parent_hierarchy":"/meta/ns"}}],"must_not":[],"should":[]}},"size":$limit}"""
    val json = HttpUtil.getJson(s"http://$httpAddress/_search", query, "text/plain")

    // We expect to get all the prefixes in one request, so check if it doesn't work out.
    assert(json.findValue("hits").findValue("total").asInt <= limit)

    // Only interested in paths of the form: /meta/ns/<hash>
    //
    val pathPattern = "^/meta/ns/([^/]+)$".r

    json.findValue("hits").findValue("hits").elements.asScala.flatMap { infoton =>

      val path = infoton.findValue("system").findValue("path").asText

      path match {
        case pathPattern(nsHash) =>
          val fields = infoton.findValue("fields")
          val url = fields.findValue("url").get(0).asText

          Some(nsHash -> url)
        case _ => None
      }
    }
      .toMap
  }
}
