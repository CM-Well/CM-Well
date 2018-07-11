package cmwell.analytics.util

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream.ActorMaterializer
import com.fasterxml.jackson.databind.JsonNode

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor
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
  def es(url: String)
        (implicit system: ActorSystem,
         executionContext: ExecutionContextExecutor,
         actorMaterializer: ActorMaterializer): String = {

    val uri = Uri(url)

    val request = HttpRequest(
      method = GET,
      uri = s"http://${uri.authority.host}:${uri.authority.port}/proc/health?format=json")

    val json: JsonNode = HttpUtil.jsonResult(request, "fetch /proc/health")

    val masterIpAddresses: Seq[String] = json.get("fields").findValue("masters").elements.asScala.map(_.textValue).toSeq

    if (masterIpAddresses.isEmpty)
      throw new RuntimeException("No master node addresses found.")

    // For Elasticsearch, the port is 9201 for a single node, and 9200 for clustered.
    val esPort = if (masterIpAddresses.lengthCompare(1) > 0) "9200" else "9201"

    // All the masters should be accessible, but verify that.
    // A better implementation would keep all the endpoints in the list, and we could fall back to the others
    // if the one we are using disappears.
    val firstAccessibleESEndpoint = masterIpAddresses.find { ipAddress =>
      val request = HttpRequest(
        method = GET,
        uri = s"http://$ipAddress:$esPort")

      Try(HttpUtil.result(request, "probe for accessible es endpoint")).isSuccess
    }

    if (firstAccessibleESEndpoint.isEmpty)
      throw new RuntimeException("No accessible ES endpoint was found.")

    s"${firstAccessibleESEndpoint.get}:$esPort"
  }
}
