package cmwell.analytics.data

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object InfotonAndPathWithSystemFields extends EstimateDatasetSize {

  // TODO: implement column expressions for showing well-formedness and consistency


  // We expect the join to be 1:1, so simply adding the two estimates makes sense.
  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    InfotonWithSystemFields.estimateDatasetSize + PathWithKeyFields.estimateDatasetSize


  /**
    * Create a dataset that joins the infoton table (system fields only) with the path table.
    */
  def apply()(implicit spark: SparkSession): DataFrame = {

    // Assume: We are reading the entire dataset here (need better estimation for subsets).
    // To ensure that we maintain stability (i.e, no OOM) and to join efficiently, calculate a partition size
    // where the size of data in each partition is approximately the ideal partition size.
    val numPartitions = Spark.idealPartitioning(estimateDatasetSize)

    // Get the two Datasets and repartition them along the uuid join key so that the target dataset
    // will be (approximately) the ideal partition size for Spark.

    def prefixColumns[T](dataset: Dataset[T], prefix: String): DataFrame =
      dataset.select(dataset.columns.map(columnName => dataset(columnName).as(prefix + columnName)):_*)

    val infotonDataset = prefixColumns(InfotonWithSystemFields(), "infoton").as("infoton")
    val repartitionedInfotonDataset = infotonDataset.repartition(numPartitions, infotonDataset("infoton_uuid"))

    val pathDataset = prefixColumns(PathWithKeyFields(), "path").as("path")
    val repartitionedPathDataset = pathDataset.repartition(numPartitions, pathDataset("path_uuid"))

    // We do a full join so that we can find pairs where one or the other size is missing.

    repartitionedInfotonDataset.join(repartitionedPathDataset,
      joinExprs = repartitionedInfotonDataset("infoton_uuid") === repartitionedPathDataset("path_uuid"),
      joinType = "full_outer")
  }
}
