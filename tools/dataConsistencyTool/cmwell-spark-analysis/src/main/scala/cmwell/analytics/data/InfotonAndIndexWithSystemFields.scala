package cmwell.analytics.data

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import scala.collection.immutable.ListMap

// TODO: This needs to be reworked to use the separate ES downloader

object InfotonAndIndexWithSystemFields extends EstimateDatasetSize {

  // We expect the join to be 1:1, so simply adding the two estimates makes sense.
  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    InfotonWithSystemFields.estimateDatasetSize + IndexWithSystemFields.estimateDatasetSize


  // We have the same uuid from both tables. Should be a no-op since that is the join key.
  def isUuidConsistent(dataset: DataFrame): Column =
    dataset("infoton_uuid") === dataset("index_uuid")

  // Both indexTime fields are Longs, but the representation from infoton is as a string
  def isIndexTimeConsistent(dataset: DataFrame): Column =
    dataset("infoton_indexTime").cast(DataTypes.LongType) === dataset("index_indexTime")

  // The Infoton type uses a different coding scheme between tables, so we need to convert before comparing.
  def isTypeConsistent(dataset: DataFrame): Column =
    dataset("infoton_type") === lower(substring(dataset("index_kind"), 0, 1))

  // In the infoton table, lastModified is an ISO 8601 timestamp with millisecond precision.
  // In the index, lastModified is in the form: "Wed Jan 17 16:53:04 EST 2018" (and millisecond precision is lost).
  def isLastModifiedConsistent(dataset: DataFrame): Column =
    Constraints.areTimestampsConsistent(
      iso8601Column = dataset("infoton_lastModified"),
      millisColumn = dataset("index_lastModified"))

  def isPathConsistent(dataset: DataFrame): Column =
    dataset("infoton_path") === dataset("index_path")

  def isDcConsistent(dataset: DataFrame): Column =
    dataset("infoton_dc") === dataset("index_dc")

  def isWellFormed(dataset: DataFrame): Column =
    InfotonWithSystemFields.isWellFormed(dataset, "infoton_") &&
      IndexWithSystemFields.isWellFormed(dataset, "index_")

  // This only makes sense to be applied with well-formedness constraints, or on data that is known to be well-formed.
  def isConsistent(dataset: DataFrame): Column =
    InfotonWithSystemFields.isConsistent(dataset, "infoton_") && IndexWithSystemFields.isConsistent(dataset, "index_") &&
      isUuidConsistent(dataset) &&
      isIndexTimeConsistent(dataset) &&
      isTypeConsistent(dataset) &&
      isLastModifiedConsistent(dataset) &&
      isPathConsistent(dataset) &&
      isDcConsistent(dataset)

  def constraints(dataset: DataFrame): ListMap[String, Column] = {
    val joinedConstraints = ListMap(
      "isWellFormed" -> isWellFormed(dataset),

      "isConsistent" -> isConsistent(dataset),

      "isUuidConsistent" -> isUuidConsistent(dataset),
      "isIndexTimeConsistent" -> isIndexTimeConsistent(dataset),
      "isTypeConsistent" -> isTypeConsistent(dataset),
      "isLastModifiedConsistent" -> isLastModifiedConsistent(dataset),
      "isPathConsistent" -> isPathConsistent(dataset),
      "isDcConsistent" -> isDcConsistent(dataset)
    )

    val infotonConstraints = InfotonWithSystemFields.constraints(dataset, "infoton_")
    val indexConstraints = IndexWithSystemFields.constraints(dataset, "index_")

    joinedConstraints ++ infotonConstraints ++ indexConstraints
  }


  /**
    * Create a dataset that joins the infoton table (system fields only) with the path table.
    */
  def apply(esExtractPath: Option[String] = None)(implicit spark: SparkSession): DataFrame = {

    // Assume: We are reading the entire dataset here (need better estimation for subsets).
    // To ensure that we maintain stability (i.e, no OOM) and to join efficiently, calculate a partition size
    // where the size of data in each partition is approximately the ideal partition size.
    val numPartitions = Spark.idealPartitioning(estimateDatasetSize)

    // Get the two Datasets and repartition them along the uuid join key so that the target dataset
    // will be (approximately) the ideal partition size for Spark.

    // This join was originally done using joinWith, but that seems to cause spurious (wrong) shuffles to be added.
    // We rename the columns with a prefix for each table to preserve the identity of the columns.

    def prefixColumns[T](dataset: Dataset[T], prefix: String): DataFrame =
      dataset.select(dataset.columns.map(columnName => dataset(columnName).as(prefix + columnName)): _*)

    val infotonDataset = prefixColumns(InfotonWithSystemFields(), "infoton_").as("infoton")
    val repartitionedInfotonDataset = infotonDataset.repartition(numPartitions, infotonDataset("infoton_uuid"))

    // If a dataset was not provided (e.g., saved to parquet using the extract-index-from-es tool),
    // then use the (unreliable) ES Spark connector to get the data.
    val indexWithSystemFieldsRaw: Dataset[Row] =
      if (esExtractPath.isEmpty)
        IndexWithSystemFields().toDF()
      else
        spark.read.parquet(esExtractPath.get)

    val indexDataset = prefixColumns(indexWithSystemFieldsRaw, "index_").as("index")

    val repartitionedIndexDataset = indexDataset.repartition(numPartitions, indexDataset("index_uuid"))

    // We do a full join so that we can find pairs where one or the other size is missing.

    repartitionedInfotonDataset.join(repartitionedIndexDataset,
      joinExprs = repartitionedInfotonDataset("infoton_uuid") === repartitionedIndexDataset("index_uuid"),
      joinType = "full_outer")
  }
}
