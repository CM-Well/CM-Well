package cmwell.analytics.data

import cmwell.analytics.util.CassandraSystem
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

case class InfotonWithUuidOnly(uuid: String)


object InfotonWithUuidOnly extends EstimateDatasetSize {

  private val BytesPerRow: Int = 8 + 8 + 32 // null mask, fixed part, variable (uuid) part

  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    CassandraSystem.rowCount(table = "infoton") * BytesPerRow


  case class Columns(uuid: Column) {

    def this(dataset: DataFrame, prefix: String = "") = this(
      uuid = dataset(prefix + "uuid"))
  }

  def isWellFormed(infotons: DataFrame, prefix: String = ""): Column = {

    val columns = new Columns(infotons, prefix)

    Constraints.isUuidWellFormed(columns.uuid)
  }


  /**
    * Get a Dataset[InfotonWithUuidOnly].
    * This represents data from the path2.infoton table from Casssandra, including only the system fields.
    */
  def apply()
           (implicit spark: SparkSession): Dataset[InfotonWithUuidOnly] = {

    val infotonRdd: CassandraTableScanRDD[CassandraRow] = spark.sparkContext.cassandraTable("data2", "infoton")
      .select("uuid")
      .where("quad = ?", "cmwell://meta/sys") // only look at system fields
      // TODO: In a completely rigorous check, we would leave the following constraint out, even if it means retrieving more data.
      .where("field = ?", "path") // Assume system fields for every infoton includes a 'path' field.

    // Map the grouped data to Infoton objects containing only the uuid field.
    import spark.implicits._
    val objectRDD: RDD[InfotonWithUuidOnly] = infotonRdd.map { row =>
      InfotonWithUuidOnly(uuid = row.getString("uuid")) }

    spark.createDataset(objectRDD)
  }
}