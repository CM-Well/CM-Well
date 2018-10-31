package cmwell.analytics.data

import cmwell.analytics.util.{CassandraSystem, DatasetFilter, KeyFields}
import com.datastax.spark.connector._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.joda.time.format.ISODateTimeFormat


object InfotonWithKeyFields extends EstimateDatasetSize {

  private val BytesPerRow: Int = 8 + (8 * 7) + (32 + 8 + 32) // null mask, fixed part, variable part (estimate)


  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    CassandraSystem.rowCount(table = "infoton") * BytesPerRow

  /**
    * Get a Dataset[KeyFields] from the Infoton table.
    * This represents data from the path2.infoton table from Casssandra, including only the
    * uuid, lastModified and path.
    */
  def apply(datasetFilter: Option[DatasetFilter] = None)
           (implicit spark: SparkSession): Dataset[KeyFields] = {

    val infotonRdd = spark.sparkContext.cassandraTable("data2", "infoton")
      .select("uuid", "field", "value")
      .where("quad = ?", "cmwell://meta/sys") // only look at system fields
      // We can't further restrict system fields here due to CQL limitations.
      .spanBy(row => row.getString("uuid"))

    // Map the grouped data to Infoton objects containing only the system fields.
    val objectRDD = infotonRdd.map { case (uuid, fields) =>
      var lastModified: java.sql.Timestamp = null
      var path: String = null

      fields.foreach(field => field.getString("field") match {
        case "lastModified" => lastModified = new java.sql.Timestamp(
          ISODateTimeFormat.dateTime.parseDateTime(field.getString("value")).getMillis)

        case "path" => path = field.getString("value")

        case _ =>
        // Ignore other system fields
      })

      KeyFields(
        uuid = uuid,
        lastModified = lastModified,
        path = path)
    }


    import spark.implicits._
    val ds = spark.createDataset(objectRDD)

    datasetFilter.fold(ds)(_.applyFilter(ds, forAnalysis = false))
  }
}
