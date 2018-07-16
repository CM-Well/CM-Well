package cmwell.analytics.util

import com.fasterxml.jackson.databind.JsonNode

import scala.collection.JavaConverters._
import scala.collection.breakOut

case class Shard(indexName: String, shard: Int)

case class EsTopology(alias: String,
                      nodes: Map[String, String], // nodeId -> address
                      shards: Map[Shard, Seq[String]]) // shard -> Seq[nodeId]

/**
  * This object provides methods that discover the topology of an ES instance, and allows mapping an alias
  * to the individual shards that compose it, along with the addresses of the nodes where those
  * shards can be found.
  */
object DiscoverEsTopology {

  def apply(esContactPoint: String, alias: String): EsTopology = {

    val nodesJson = HttpUtil.getJson(s"http://$esContactPoint/_nodes")
    val searchShardsJson = HttpUtil.getJson(s"http://$esContactPoint/_search_shards")
    val aliasesJson = HttpUtil.getJson(s"http://$esContactPoint/$alias/_alias")

    val extractAddress = "inet\\[/(.+)]".r // "inet[/0.0.0.0:0000]"
    val nodes: Map[String, String] = nodesJson.get("nodes").fields.asScala.map { entry =>

      val nodeId = entry.getKey
      val extractAddress(hostPort) = entry.getValue.get("http_address").asText

      nodeId -> hostPort
    }.toMap

    val shards: Map[Shard, Seq[String]] = searchShardsJson.get("shards").elements.asScala.map { shardLocations: JsonNode =>

      // Sort the shard locations so that the primary is first - we will always try the primary first
      val locations = shardLocations.elements.asScala.toSeq.sortBy(_.findValue("primary").booleanValue).reverse

      assert(locations.nonEmpty)
      assert(locations.head.findValue("primary").booleanValue) // first one is primary node

      val indexName = locations.head.findValue("index").asText
      val shard = locations.head.findValue("shard").asInt
      val nodeIds: Vector[String] = locations.map(_.findValue("node").asText)(breakOut)

      Shard(indexName, shard) -> nodeIds
    }.toMap

    val indexNames = aliasesJson.fieldNames.asScala.toSet

    EsTopology(
      alias = alias,
      nodes = nodes,
      // Only include shards for indexes that are included in the given alias
      shards = shards.filter { case (shard, _) => indexNames.contains(shard.indexName) })
  }
}
