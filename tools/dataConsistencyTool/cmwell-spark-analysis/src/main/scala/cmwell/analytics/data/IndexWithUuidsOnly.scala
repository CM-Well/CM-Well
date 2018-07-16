package cmwell.analytics.data

import cmwell.analytics.util.EsUtil
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.elasticsearch.spark._

case class IndexWithUuidsOnly(uuid: String)

/**
  * This method of extracting data from the Elasticsearch index (i.e., using the ES Spark connector)
  * does not work consistently (perhaps due to old version of ES?). Use this method of retrieving index data
  * at your own risk.
  */
object IndexWithUuidsOnly extends EstimateDatasetSize {

  private val BytesPerRow: Int = 8 + 8 + 32 // null mask, fixed part, variable part

  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    EsUtil.countCmWellAllDocuments() * BytesPerRow

  case class Columns(uuid: Column) {

    def this(dataset: DataFrame, prefix: String = "") = this(
      uuid = dataset(prefix + "uuid"))
  }


  def isWellFormed(dataset: DataFrame, prefix: String = ""): Column = {

    val columns = new Columns(dataset, prefix)

    Constraints.isUuidWellFormed(columns.uuid)
  }

  def queryWithFields(currentOnly: Boolean = false): String =
    if (currentOnly)
      """{"query":{"term":{"current":true}},"fields":[]}"""
    else
      """{"query":{"match_all":{}},"fields":[]}"""

  def queryWithoutFields(currentOnly: Boolean = false): String =
    if (currentOnly)
      """{"query":{"term":{"current":true}}}"""
    else
      """{"query":{"match_all":{}}}"""

  def apply(currentOnly: Boolean = false)(implicit spark: SparkSession): Dataset[IndexWithUuidsOnly] = {

    import spark.implicits._

    spark.createDataset(
      spark.sparkContext.esRDD(
        resource = "cm_well_all",
        query = queryWithFields(currentOnly),
        cfg = Map("es.read.field.exclude" -> "*")
      )
        .map { case (uuid, _) => IndexWithUuidsOnly(uuid = uuid) }
    )
  }
}
