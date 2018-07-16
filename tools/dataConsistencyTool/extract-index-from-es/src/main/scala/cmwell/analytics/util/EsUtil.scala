package cmwell.analytics.util

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.util.ByteString

import scala.concurrent.ExecutionContextExecutor

object EsUtil {

  def countDocumentsInShard(httpAddress: String,
                            shard: Shard,
                            filter: String)
                           (implicit system: ActorSystem,
                            executionContext: ExecutionContextExecutor,
                            actorMaterializer: ActorMaterializer): Long = {

    val request = HttpRequest(
      method = HttpUtil.SAFE_POST,
      uri = s"http://$httpAddress/${shard.indexName}/_count?preference=_shards:${shard.shard}",
      entity = ByteString(s"{$filter}"))

    val json = HttpUtil.jsonResult(request, "count documents in shard")

    json.get("count").asLong
  }
}
