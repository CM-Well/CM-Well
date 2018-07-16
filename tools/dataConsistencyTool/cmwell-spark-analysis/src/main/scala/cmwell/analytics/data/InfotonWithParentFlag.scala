package cmwell.analytics.data

import cmwell.analytics.util.CassandraSystem
import com.datastax.spark.connector._
import org.apache.spark.sql.{Dataset, SparkSession}


case class InfotonWithParentFlag(uuid: String, isParent: Boolean)

object InfotonWithParentFlag extends EstimateDatasetSize {

  private val BytesPerRow: Int = 8 + (8 * 2) + 32 // null mask, fixed part, variable part


  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    CassandraSystem.rowCount(table = "infoton") * BytesPerRow

  /**
    * Get a Dataset[InfotonWithParentFlag].
    * This represents data from the path2.infoton table from Casssandra, including only the
    * uuid, and a flag column that indicates whether the infoton represents a parent.
    * Parent infotons can be distinguished by the fact that they have no fields.
    */
  def apply()
           (implicit spark: SparkSession): Dataset[InfotonWithParentFlag] = {

    val infotonRdd = spark.sparkContext.cassandraTable("data2", "infoton")
      .select("uuid", "quad")
      .spanBy(row => row.getString("uuid"))

    // Map the grouped data to Infoton objects containing only the system fields.
    val objectRDD = infotonRdd.map { case (uuid, rows) =>

      // If there is no field that is not a system field, then it is a parent.
      val isParent = !rows.exists(row => row.getString("quad") != "cmwell://meta/sys")

      InfotonWithParentFlag(
        uuid = uuid,
        isParent = isParent)
    }

    import spark.implicits._
    spark.createDataset(objectRDD)
  }
}
