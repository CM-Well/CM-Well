package cmwell.analytics.data

import cmwell.analytics.util.Shard

import scala.collection.mutable

class XORSummaryFactory extends DataWriterFactory[IndexWithSourceHash]{

  val shardSummaries = mutable.Map.empty[Shard, XORSummary]

  override def apply(shard: Shard): DataWriter[IndexWithSourceHash] = shardSummaries.synchronized {
    val xorSummary = XORSummary(index = shard.indexName, shard = shard.shard.toString)
    shardSummaries += shard -> xorSummary
    xorSummary
  }
}
