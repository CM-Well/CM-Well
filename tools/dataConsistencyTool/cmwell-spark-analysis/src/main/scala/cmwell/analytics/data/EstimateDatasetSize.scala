package cmwell.analytics.data

import org.apache.spark.sql.SparkSession

/** A trait for Dataset classes that can estimate the size of their Dataset representation (in Tungsten format). */
trait EstimateDatasetSize {
  def estimateDatasetSize(implicit spark: SparkSession): Long
}
