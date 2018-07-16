package cmwell.analytics.data

import cmwell.analytics.util.CassandraSystem
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

case class PathWithUuidOnly(uuid: String)


object PathWithUuidOnly extends EstimateDatasetSize {

  private val BytesPerRow: Int = 8 + 8 + 32 // null mask, fixed part, variable (uuid) part

  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    CassandraSystem.rowCount(table = "path") * BytesPerRow


  case class Columns(uuid: Column) {

    def this(dataset: DataFrame, prefix: String = "") = this(
      uuid = dataset(prefix + "uuid"))
  }

  def isWellFormed(infotons: DataFrame, prefix: String = ""): Column = {

    val columns = new Columns(infotons, prefix)

    Constraints.isUuidWellFormed(columns.uuid)
  }


  /**
    * Get a Dataset[PathWithUuidOnly].
    * This represents data from the path2.infoton table from Casssandra, including only the system fields.
    */
  def apply()
           (implicit spark: SparkSession): Dataset[PathWithUuidOnly] = {

    val infotonRdd = spark.sparkContext.cassandraTable("data2", "path")
      .select("uuid")

    // Map the grouped data to Infoton objects containing only the uuid field.
    import spark.implicits._
    val objectRDD: RDD[PathWithUuidOnly] = infotonRdd.map { row =>
      PathWithUuidOnly(uuid = row.getString("uuid")) }

    spark.createDataset(objectRDD)
  }
}