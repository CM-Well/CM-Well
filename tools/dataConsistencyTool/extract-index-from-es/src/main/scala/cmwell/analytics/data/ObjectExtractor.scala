package cmwell.analytics.data

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import cmwell.analytics.util.TimestampConversion.convertToString

trait ObjectExtractor[T <: GenericRecord] {

  val schema: Schema

  /**
    * Extracts an object of type T from a hit in a fetch result.
    *
    * @param hit An (inner) _hit element in a fetch result.
    * @return An object of type T
    */
  def extractFromJson(hit: JsonNode): T


  private def currentFilter(current: Boolean): String =
    s""" "term": { "system.current": $current }"""

  private def lastModifiedGteFilter(timestamp: java.sql.Timestamp): String =
    s""" "range": { "system.lastModified": { "gte": "${convertToString(timestamp)}" } } """

  private def pathPrefixFilter(pathPrefix: String): String =
    s""" "prefix": { "system.path": "$pathPrefix" } """

  private def orFilters(filter1: String, filter2: String): String =
    s""" "or": [ { $filter1 }, { $filter2 } ] """

  private def andFilters(filter1: String, filter2: String): String =
    s""" "and": [ { $filter1 }, { $filter2 } ] """

  private def noFilter: String = """ "match_all": { }"""

  // This template does not include the outer braces - they will be added by the caller.
  private def queryTemplate(filter: String): String =
    s""" "query": { "filtered" : { "filter" : { $filter } } }"""


  /**
    * Filtering is done to reduce the amount of data that needs to be retrieved from ES.
    * We always use filtering (as opposed to queries) since filtering is faster (we don't care about scoring).
    *
    * When filtering on current and lastModified, we conceptually OR the filters together for the extract,
    * This is because current and lastModified are conceptually on the same (temporal) axis.
    * Since the extract might be used for multiple purposes (e.g., duplicate-current-index and uuid-set-comparison),
    * the specific use of that extract should apply (one of the) filters again.
    * For example, if both current and lastModified were filtered (ORed together),
    * then a duplicate-current-index analysis should always filter its dataset
    * using current=true, since the extract could include rows with current=false but have a recent lastModified.
    *
    * When we do the analysis part, we would conceptually AND any temporal predicates, but the reality is that
    * one or the other of current and lastModifiedGte would be used in any given analysis.
    */
  def filter(currentOnly: Boolean,
             pathPrefix: Option[String],
             lastModifiedGte: Option[java.sql.Timestamp]): String = {

    // The --current-only parameter works differently from other filters, for both historical reasons
    // and because of the inconsistent way that Scallop handles Boolean options.
    // currentOnly == true => filter on current==true
    // currentOnly == false => no filtering on current (NOT: filter on current==false)

    val prefixFilterClause = pathPrefix.map(pathPrefixFilter)
    val lastModifiedGteClause = lastModifiedGte.map(lastModifiedGteFilter)

    val temporalFilterClause = if (currentOnly)
      lastModifiedGteClause match {
        case Some(lm) => Some(orFilters(currentFilter(true), lm))
        case _ => Some(currentFilter(true))
      }
    else {
      lastModifiedGteClause match {
        case Some(lm) => Some(lm)
        case _ => None
      }
    }

    val filters = temporalFilterClause match {
      case Some(t) => prefixFilterClause match {
        case Some(p) => andFilters(p, t)
        case _ => t
      }
      case _ => prefixFilterClause match {
        case Some(p) => p
        case _ => noFilter
      }
    }

    queryTemplate(filters)
  }

  /** A comma-delimited list of field names that are to be fetched from ES */
  def includeFields: String

  /** An estimate of the size of one infoton's worth of JSON data, as returned from ES */
  def infotonSize: Int
}


