package cmwell.analytics.data

import cmwell.analytics.util.CassandraSystem
import com.datastax.spark.connector._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import scala.collection.immutable.ListMap

/**
  * This is a view of an infoton (of any type) that only includes basic system fields that are common to all
  * infoton types.
  *
  * Note that `uuid` is not nullable since that represents identity. Other fields should always be present in a
  * well-formed infoton, but we do not make that presumption here. In the path2.infoton table, all values in the
  * 'value' column are Strings, and therefore nullable. The representation is intended to represent how the data
  * is actually stored in C*.
  *
  * The field names match the values in the 'field' column.
  */
case class InfotonWithSystemFields(uuid: String,
                                   dc: String,
                                   indexName: String,
                                   indexTime: String,
                                   lastModified: String,
                                   path: String,
                                   `type`: String)

object InfotonWithSystemFields extends EstimateDatasetSize {

  private val BytesPerRow: Int = 8 + (8 * 7) + (32 + 8 + 8 + 8 + 8 + 16 + 1) // null mask, fixed part, variable part (estimate)

  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    CassandraSystem.rowCount(table = "infoton") * BytesPerRow


  case class Columns(uuid: Column,
                     dc: Column,
                     indexName: Column,
                     indexTime: Column,
                     lastModified: Column,
                     path: Column,
                     `type`: Column) {

    def this(dataset: DataFrame, prefix: String = "") = this(
      uuid = dataset(prefix + "uuid"),
      dc = dataset(prefix + "dc"),
      indexName = dataset(prefix + "indexName"),
      indexTime = dataset(prefix + "indexTime"),
      lastModified = dataset(prefix + "lastModified"),
      path = dataset(prefix + "path"),
      `type` = dataset(prefix + "type"))
  }

  def isWellFormed(infotons: DataFrame, prefix: String = ""): Column = {
    val columns = new Columns(infotons, prefix)

    Constraints.isUuidWellFormed(columns.uuid) &&
      Constraints.isDcWellFormed(columns.dc) &&
      Constraints.isIndexNameWellFormed(columns.indexName) &&
      Constraints.isIndexTimeCasWellFormed(columns.indexTime) &&
      Constraints.isLastModifiedCasWellFormed(columns.lastModified) &&
      Constraints.isPathWellFormed(columns.path) &&
      Constraints.isTypeWellFormed(columns.`type`)
  }
  // An infoton cannot be indexed before it is modified.
  // Do the comparison by converting to seconds since epoch.
  // Note that this conversion truncates available millisecond precision from both columns.
  // It is possible to preserve this precision by converting date strings using an UDF.
  def areTimestampsConsistent(lastModified: Column, indexTime: Column): Column =
    (indexTime.cast(LongType) / 1000).cast(LongType) >= TimestampConversion.convertISO8601ToDate(lastModified).cast(LongType)

  // The Infoton table is only consistent when it has been indexed (and these fields are set)
  def isIndexed(indexName: Column, indexTime: Column): Column =
    indexName.isNotNull && indexTime.isNotNull

  // Consistency only makes sense when applied in combination with well-formedness constraints,
  // or on data that is known to be well-formed.
  def isConsistent(infotons: DataFrame, prefix: String = ""): Column = {

    val columns = new Columns(infotons, prefix)

    isIndexed(indexName = columns.indexTime, indexTime = columns.indexTime) && // Consistent when infoton has been indexed.
    areTimestampsConsistent(columns.lastModified, columns.indexTime)
  }

  def constraints(dataset: DataFrame, prefix: String): ListMap[String, Column] = {
    val columns = new Columns(dataset, prefix)

    ListMap(
      (prefix + "isUuidNull") -> columns.uuid.isNull,
      (prefix + "isDcNull") -> columns.dc.isNull,
      (prefix + "isIndexNameNull") -> columns.indexName.isNull,
      (prefix + "isIndexTimeNull") -> columns.indexTime.isNull,
      (prefix + "isLastModifiedNull") -> columns.lastModified.isNull,
      (prefix + "isPathNull") -> columns.path.isNull,
      (prefix + "isTypeNull") -> columns.`type`.isNull,

      (prefix + "isWellFormed") -> isWellFormed(dataset, prefix),

      (prefix + "isUuidWellFormed") -> Constraints.isUuidWellFormed(columns.uuid),
      (prefix + "isDcWellFormed") -> Constraints.isDcWellFormed(columns.dc),
      (prefix + "isIndexNameWellFormed") -> Constraints.isIndexNameWellFormed(columns.indexName),
      (prefix + "isIndexTimeWellFormed") -> Constraints.isIndexTimeCasWellFormed(columns.indexTime),
      (prefix + "isLastModifiedWellFormed") -> Constraints.isLastModifiedCasWellFormed(columns.lastModified),
      (prefix + "isPathWellFormed") -> Constraints.isPathWellFormed(columns.path),
      (prefix + "isTypeWellFormed") -> Constraints.isTypeWellFormed(columns.`type`),

      (prefix + "isConsistent") -> isConsistent(dataset, prefix),

      (prefix + "areTimestampsConsistent") -> areTimestampsConsistent(lastModified = columns.lastModified, indexTime = columns.indexTime),
      (prefix + "isIndexed") -> isIndexed(indexName = columns.indexName, indexTime = columns.indexTime)
    )
  }


  /**
    * Get a Dataset[InfotonWithSystemFields].
    * This represents data from the path2.infoton table from Casssandra, including only the system fields.
    */
  def apply()
           (implicit spark: SparkSession): Dataset[InfotonWithSystemFields] = {

    val infotonRdd = spark.sparkContext.cassandraTable("data2", "infoton")
      .select("uuid", "field", "value")
      .where("quad = ?", "cmwell://meta/sys") // only look at system fields
      // We can't further restrict system fields here due to CQL limitations.
      .spanBy(row => row.getString("uuid"))

    // Map the grouped data to Infoton objects containing only the system fields.
    val objectRDD = infotonRdd.map { case (uuid, fields) =>
      var dc: String = null
      var indexName: String = null
      var indexTime: String = null
      var lastModified: String = null
      var path: String = null
      var `type`: String = null

      fields.foreach(field => field.getString("field") match {
        case "dc" => dc = field.getString("value")
        case "indexName" => indexName = field.getString("value")
        case "indexTime" => indexTime = field.getString("value")
        case "lastModified" => lastModified = field.getString("value")
        case "path" => path = field.getString("value")
        case "type" => `type` = field.getString("value")

        case _ =>
        // Ignore other system fields (related to data, links):
        // mimeType, contentLength, data, contentPointer, linkTo, linkType
      })

      InfotonWithSystemFields(
        uuid = uuid,
        dc = dc,
        indexName = indexName,
        indexTime = indexTime,
        lastModified = lastModified,
        path = path,
        `type` = `type`)
    }

    import spark.implicits._
    spark.createDataset(objectRDD)
  }
}
