package cmwell.analytics.data

import cmwell.analytics.util.{CassandraSystem, KeyFields}
import com.datastax.spark.connector._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object PathWithKeyFields extends EstimateDatasetSize {

  private val BytesPerRow = 8 + (3 * 8) + (16 + 8 + 32) // bit mask, fixed, variable

  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    CassandraSystem.rowCount(table = "path") * BytesPerRow


  case class Columns(path: Column,
                     lastModified: Column,
                     uuid: Column) {

    def this(dataset: DataFrame, prefix: String = "") = this(
      path = dataset(prefix + "path"),
      lastModified = dataset(prefix + "lastModified"),
      uuid = dataset(prefix + "uuid"))
  }

  def isWellFormed(dataset: DataFrame, prefix: String = ""): Column = {

    val columns = new Columns(dataset, prefix)

    Constraints.isPathWellFormed(columns.path) &&
      Constraints.isLastModifiedCasWellFormed(columns.lastModified) &&
      Constraints.isUuidWellFormed(columns.uuid)
  }


  def apply()
           (implicit spark: SparkSession): Dataset[KeyFields] = {

    val infotonRdd = spark.sparkContext.cassandraTable("data2", "path")
      .select("path", "last_modified", "uuid")

    // Map the grouped data to Infoton objects containing only the system fields.
    val objectRDD = infotonRdd.map { cassandraRow =>

      KeyFields(
        path = cassandraRow.getString("path"),
        lastModified = new java.sql.Timestamp(cassandraRow.getDateTime("last_modified").getMillis),
        uuid = cassandraRow.getString("uuid"))
    }

    import spark.implicits._
    spark.createDataset(objectRDD)
  }
}
