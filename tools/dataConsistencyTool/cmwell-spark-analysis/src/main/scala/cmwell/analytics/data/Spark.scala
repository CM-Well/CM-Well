package cmwell.analytics.data

object Spark {

  // The optimal size of a Spark partition (128 MB)
  private val IdealPartitionSizeInBytes = 128 * 1024 * 1024

  def idealPartitioning(datasetSize: Long): Int =
    1 max Math.ceil(datasetSize / IdealPartitionSizeInBytes).toInt
}
