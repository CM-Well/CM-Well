package cmwell.analytics.util

import java.net.URI

import com.fasterxml.jackson.databind.JsonNode

import scala.collection.JavaConverters._
import scala.util.Try

// TODO: These s/b `Uri`s?
case class ContactPoints(cassandra: String,
                         es: String)

object FindContactPoints {

  /** Given a CM-Well URL, find Elasticsearch contact point.
    * This will be in the form: host:port.
    * Uses the first accessible master, which should expose ES on port 9200 (clustered) or 9201 (single).
    *
    * Using jackson included with Spark for parsing JSON (to avoid version conflicts).
    */
  def es(url: String): String = {

    val uri = new URI(url)

    val json: JsonNode = HttpUtil.getJson(s"http://${uri.getHost}:${uri.getPort}/proc/health?format=json")

    val masterIpAddresses: Seq[String] = json.get("fields").findValue("masters").elements.asScala.map(_.textValue).toSeq

    if (masterIpAddresses.isEmpty)
      throw new RuntimeException("No master node addresses found.")

    // For Elasticsearch, the port is 9201 for a single node, and 9200 for clustered.
    val esPort = if (masterIpAddresses.lengthCompare(1) > 0) "9200" else "9201"

    // All the masters should be accessible, but verify that.
    // A better implementation would keep all the endpoints in the list, and we could fall back to the others
    // if the one we are using disappears.
    val firstAccessibleESEndpoint = masterIpAddresses.find { ipAddress =>
      Try(HttpUtil.get(s"http://$ipAddress:$esPort")).isSuccess
    }

    if (firstAccessibleESEndpoint.isEmpty)
      throw new RuntimeException("No accessible ES endpoint was found.")

    s"${firstAccessibleESEndpoint.get}:$esPort"
  }

  /**
    * Get the internal contact point for one of Cassandra nodes.
    * This is not exposed using a nice JSON API, but we can pick it out of the HTML that it is embedded in.
    */
  def cas(url: String): String = {

    val uri = new URI(url)

    val json: JsonNode = HttpUtil.getJson(s"http://${uri.getAuthority}:${uri.getPort}/proc/health-detailed.md?format=json")

    val data = json.get("content").findValue("data").asText

    // The first 6 lines will looks like this (ignore them).

    //
    // ##### Current time: 2018-02-27T18:48:35.138Z
    // ### Cluster name: cm-well-???
    // ### Data was generated on:
    // | **Node** | **WS** | **BG** | **CAS** | **ES** | **ZK** | **KF** |
    // |----------|--------|--------|---------|--------|--------|--------|

    // The remaining lines will look like this:
    //|10.204.192.148|<span style='color:green'>Green</span><br>Ok<br>Response time: 3 ms|<span style='color:green'>Green</span><br>Par4: i.p:0:G i:27:G p.p:0:G p:41:G|<span style='color:green'>Green</span><br>192.168.100.58 -> UN<br>192.168.100.59 -> UN<br>192.168.100.57 -> UN<br>192.168.100.60 -> UN|<span style='color:green'>Green</span><br>||<span style='color:green'>Green</span><br>|

    // We want what is in the fourth column.
    // <span style='color:green'>Green</span><br>10.204.146.186 -> UN<br>10.204.146.187 -> UN<br>10.204.146.188 -> UN<br>10.204.146.185 -> UN

    val allInternalAddresses = for {
      line <- data.split("\\n").drop(6)
      casColumn = line.split("\\|")(4)
      internalAddresses = casColumn.drop(casColumn.indexOf("</span>") + "</span>".length)
      eachAddress <- internalAddresses.split("<br>")
      if !eachAddress.isEmpty
    } yield eachAddress.take(eachAddress.indexOf(" "))

    if (allInternalAddresses.isEmpty)
      throw new RuntimeException("No Cassandra endpoints found.")

    allInternalAddresses.head
  }
}
