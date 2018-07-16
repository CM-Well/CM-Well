package cmwell.analytics.util

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import com.fasterxml.jackson.databind.JsonNode

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.concurrent.ExecutionContextExecutor

case class Shard(indexName: String, shard: Int) {

  // This field is not in the constructor argument list since it is not part of equality.
  private var _downloadAttempt: Int = 0

  def downloadAttempt: Int = _downloadAttempt

  def nextAttempt: Shard = {
    val copy = this.copy()
    copy._downloadAttempt = this._downloadAttempt + 1
    copy
  }
}


case class EsTopology(nodes: Map[String, String], // nodeId -> address
                      shards: Map[Shard, Seq[String]], // shard -> Seq[nodeId]
                      allIndexNames: Set[String])

/**
  * This object provides methods that discover the topology of an ES instance, and allows mapping an alias
  * to the individual shards that compose it, along with the addresses of the nodes where those
  * shards can be found.
  *
  * The topology returned will only include entries for indexes that are to be read.
  */
object DiscoverEsTopology {

  def apply(esContactPoint: String,
            aliases: Seq[String] = Seq.empty)
           (implicit system: ActorSystem,
            executionContext: ExecutionContextExecutor,
            actorMaterializer: ActorMaterializer): EsTopology = {

    // Get a map from node name -> address

    val nodesJson = HttpUtil.jsonResult(HttpRequest(uri = s"http://$esContactPoint/_nodes"), "find es nodes")
    val extractAddress = "inet\\[/(.+)]".r // "inet[/10.204.146.152:9304]"
    val nodes: Map[String, String] = nodesJson.get("nodes").fields.asScala.map { entry =>

      val nodeId = entry.getKey
      val extractAddress(hostPort) = entry.getValue.get("http_address").asText

      nodeId -> hostPort
    }.toMap

    // Find all the shards for all indexes.

    val searchShardsJson = HttpUtil.jsonResult(HttpRequest(uri = s"http://$esContactPoint/_search_shards"), "search shards")

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

    // Get a list of aliases that we want to read from.
    // This is used to filter the list of all shards down to the ones that we want to read from.

    def resolveAlias(alias: String): Set[String] = {
      val aliasesJson = HttpUtil.jsonResult(HttpRequest(uri = s"http://$esContactPoint/$alias/_alias"), s"shards for $alias")
      aliasesJson.fieldNames.asScala.toSet
    }

    val readIndexNames: Set[String] = if (aliases.isEmpty)
      resolveAlias("cm_well_all") // Default if no alias or index name specified.
    else
      (Set.empty[String] /: aliases) (_ ++ resolveAlias(_)) // resolve and combine all the index names

    // allIndexNames is useful for validation of parameters to ensure they are all valid index names.

    val allIndexNames: Set[String] = {
      val aliasesJson = HttpUtil.jsonResult(HttpRequest(uri = s"http://$esContactPoint/_all/_alias"), "Get all index names")
      aliasesJson.fieldNames.asScala.toSet
    }

    EsTopology(
      nodes = nodes,
      // Only read shards for indexes that are included in the given aliases or index names.
      shards = shards.filter { case (shard, _) => readIndexNames.contains(shard.indexName) },
      allIndexNames = allIndexNames)
  }
}
