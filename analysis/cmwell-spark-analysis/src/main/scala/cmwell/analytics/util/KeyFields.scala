package cmwell.analytics.util

import org.apache.spark.sql.{Dataset, Row}

/**
  * This class represent the three key fields that are present in each of the infoton and path tables,
  * as well as the index.
  */
case class KeyFields(uuid: String, lastModified: java.sql.Timestamp, path: String)

object KeyFields {

  private val SampleSize = 10000

  /**
    * Estimate the size of a Dataset row in Tungsten format.
    * This is useful for calculating partition sizes. Since the path field is of variable length, it is impossible
    * to guess accurately. We can just look at a sample of rows to make an accurate estimate.
    */
  def estimateTungstenRowSize(ds: Dataset[KeyFields]): Int = {

    val samplePaths: Array[Row] = ds.select(ds("path")).take(SampleSize)

    val averagePathSize = Math.ceil((0 /: samplePaths) (_ + _.length) / samplePaths.length).toInt

    8 + // null mask
      (3 * 8) + // fixed part
      32 + // uuid
      8 + // lastModified
      averagePathSize // path
  }
}