package cmwell.analytics.data

import cmwell.analytics.util.EsUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.elasticsearch.spark._

import scala.collection.immutable.ListMap

/**
  * This method of extracting data from the Elasticsearch index (i.e., using the ES Spark connector)
  * does not work consistently (perhaps due to old version of ES?). Use this method of retrieving index data
  * at your own risk.
  */
case class IndexWithSystemFields(kind: String,
                                 uuid: String,
                                 lastModified: Option[Long],
                                 path: String,
                                 dc: String,
                                 indexName: String,
                                 indexTime: Option[Long],
                                 parent: String, // What purpose does this field serve? Can it not always be derived from path?
                                 current: Option[Boolean])

object IndexWithSystemFields extends EstimateDatasetSize {

  private val SampleSize = 10000

  /**
    * Estimate the size of a Dataset row in Tungsten format.
    * This is useful for calculating partition sizes. Since the path field is of variable length, it is impossible
    * to guess accurately. We can just look at a sample of rows to make an accurate estimate.
    */
  def estimateTungstenRowSize(ds: Dataset[IndexWithSystemFields]): Int = {

    val sample: Array[Row] = ds.toDF("kind", "uuid", "lastModified", "path", "dc", "indexName", "indexTime", "parent", "current").take(SampleSize)

    def averageLength(name: String): Int = sample.map { row =>
      val s = row.getAs[String](name)
      if (s == null) 0 else s.length
    }.sum / SampleSize

    16 + // null mask
      (8 * 9) + // fixed part
      averageLength("kind") +
      32 + // uuid
      averageLength("path") +
      averageLength("dc") +
      averageLength("indexName") +
      averageLength("parent")
  }

  private val BytesPerRow: Int = 16 + (8 * 8) + (12 + 32 + 8 + 32 + 8 + 8 + 8 + 1) // null mask, fixed part, variable part (estimate)

  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    EsUtil.countCmWellAllDocuments() * BytesPerRow

  case class Columns(kind: Column,
                     uuid: Column,
                     lastModified: Column,
                     path: Column,
                     dc: Column,
                     indexName: Column,
                     indexTime: Column,
                     parent: Column,
                     current: Column) {

    def this(dataset: DataFrame, prefix: String = "") = this(
      kind = dataset(prefix + "kind"),
      uuid = dataset(prefix + "uuid"),
      lastModified = dataset(prefix + "lastModified"),
      path = dataset(prefix + "path"),
      dc = dataset(prefix + "dc"),
      indexName = dataset(prefix + "indexName"),
      indexTime = dataset(prefix + "indexTime"),
      parent = dataset(prefix + "parent"),
      current = dataset(prefix + "current"))
  }

  def areTimestampsConsistent(indexTime: Column, lastModified: Column): Column =
    indexTime >= lastModified

  def isConsistent(dataset: DataFrame, prefix: String = ""): Column = {

    val columns = new Columns(dataset, prefix)

    areTimestampsConsistent(indexTime = columns.indexTime, lastModified = columns.lastModified)

    // TODO: Consistency: Check that parent is a prefix of path?
  }

  def isWellFormed(dataset: DataFrame, prefix: String = ""): Column = {

    val columns = new Columns(dataset, prefix)

    Constraints.isLastModifiedEsWellFormed(columns.lastModified) &&
      Constraints.isKindWellFormed(columns.kind) &&
      Constraints.isUuidWellFormed(columns.uuid) &&
      Constraints.isPathWellFormed(columns.path) &&
      Constraints.isDcWellFormed(columns.dc) &&
      Constraints.isIndexTimeEsWellFormed(columns.indexTime) &&
      Constraints.isIndexNameWellFormed(columns.indexName) &&
      Constraints.isParentWellFormed(columns.parent) &&
      Constraints.isCurrentWellFormed(columns.current)
  }

  // Use a ListMap so that the results are presented in a specific order.
  def constraints(dataset: DataFrame, prefix: String = ""): ListMap[String, Column] = {
    val columns = new Columns(dataset, prefix)

    ListMap(
      // These aren't evaluated as separate constraints, but we need to count these to interpret
      // well-formedness and consistency counts.
      (prefix + "isKindNull") -> columns.kind.isNull,
      (prefix + "isUuidNull") -> columns.uuid.isNull,
      (prefix + "isLastModifiedNull") -> columns.lastModified.isNull,
      (prefix + "isPathNull") -> columns.path.isNull,
      (prefix + "isDcNull") -> columns.dc.isNull,
      (prefix + "isIndexTimeNull") -> columns.indexTime.isNull,
      (prefix + "isIndexNameNull") -> columns.indexName.isNull,
      (prefix + "isParentNull") -> columns.parent.isNull,
      (prefix + "isCurrentNull") -> columns.current.isNull,

      (prefix + "isWellFormed") -> isWellFormed(dataset, prefix),

      (prefix + "isLastModifiedWellFormed") -> Constraints.isLastModifiedEsWellFormed(columns.lastModified),
      (prefix + "isKindWellFormed") -> Constraints.isKindWellFormed(columns.kind),
      (prefix + "isUuidWellFormed") -> Constraints.isUuidWellFormed(columns.uuid),
      (prefix + "isPathWellFormed") -> Constraints.isPathWellFormed(columns.path),
      (prefix + "isDcWellFormed") -> Constraints.isDcWellFormed(columns.dc),
      (prefix + "isIndexNameWellFormed") -> Constraints.isIndexNameWellFormed(columns.indexName),
      (prefix + "isIndexTimeEsWellFormed") -> Constraints.isIndexTimeEsWellFormed(columns.indexTime),
      (prefix + "isParentWellFormed") -> Constraints.isParentWellFormed(columns.parent),
      (prefix + "isCurrentWellFormed") -> Constraints.isCurrentWellFormed(columns.current),

      (prefix + "isConsistent") -> isConsistent(dataset, prefix),

      (prefix + "areTimestampsConsistent") -> areTimestampsConsistent(indexTime = columns.indexTime, lastModified = columns.lastModified)
    )
  }

  def apply()(implicit spark: SparkSession): Dataset[IndexWithSystemFields] = {

    val config = Map("es.read.field.include" -> "system.*")

    // Use the RDD API to get the data - it might be possible via the Dataset API, but haven't figured it out yet.
    // This will return a PairRDD, with the _id in the key, and and empty value.
    val x: RDD[IndexWithSystemFields] = spark.sparkContext.esRDD(
      resource = "cm_well_all",
      // query = ???, // TODO: Might want a parameter to allow selecting a subset (e.g., current only).
      cfg = config)
      .map { case (_, fields) =>

        val system = fields("system").asInstanceOf[scala.collection.mutable.LinkedHashMap[String, AnyRef]]

        // The map that ES provides the fields has strange use of Option that requires some explicit unwrapping.
        // The map type is LinkedHashMap[String, AnyRef], but it actually contains values that are not AnyRef (e.g., Long)!

        def extractString(name: String): String = {
          val x: Option[AnyRef] = system.get(name)

          if (x.isEmpty)
            null
          else {
            x.get match {
              case null | None => null
              case s: String => s
              case _ => throw new RuntimeException(s"Unexpected type returned in column $name: ${x.get}")
            }
          }
        }

        // Extracting date values as Long - as a java.sql.Date might be better
        def extractDate(name: String): Option[Long] = {
          val x = system.get(name)

          if (x.isEmpty)
            None
          else {
            x.get match {
              case null | None => None
              case l: AnyRef with Long => Some(l)
              case d: java.util.Date => Some(d.getTime)
              case _ => throw new RuntimeException(s"Unexpected type returned in column $name: ${x.get}")
            }
          }
        }

        def extractLong(name: String): Option[Long] = {
          val x = system.get(name)

          if (x.isEmpty)
            None
          else {
            x.get match {
              case null | None => None
              case l: AnyRef with Long => Some(l)
              case _ => throw new RuntimeException(s"Unexpected type returned in column $name: ${x.get}")
            }
          }
        }

        def extractBoolean(name: String): Option[Boolean] = {
          val x = system.get(name)

          if (x.isEmpty)
            None
          else {
            x.get match {
              case null | None => None
              case b: AnyRef with Boolean => Some(b)
              case _ => throw new RuntimeException(s"Unexpected type returned in column $name: ${x.get}")
            }
          }
        }

        IndexWithSystemFields(
          kind = extractString("kind"),
          uuid = extractString("uuid"), // TODO: Could cross-check against _id since it is the document id
          lastModified = extractDate("lastModified"),
          path = extractString("path"),
          dc = extractString("dc"),
          indexName = extractString("indexName"),
          indexTime = extractLong("indexTime"),
          parent = extractString("parent"),
          current = extractBoolean("current"))
      }

    import spark.implicits._
    spark.createDataset(x)
  }
}
