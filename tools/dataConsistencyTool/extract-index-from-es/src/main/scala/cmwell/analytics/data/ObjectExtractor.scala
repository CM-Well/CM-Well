package cmwell.analytics.data

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

trait ObjectExtractor[T <: GenericRecord] {

  val schema: Schema

  /**
    * Extracts an object of type T from a hit in a fetch result.
    * @param hit An (inner) _hit element in a fetch result.
    * @return An object of type T
    */
  def extractFromJson(hit: JsonNode): T

  def filter(currentOnly: Boolean): String =
    if (currentOnly)
      """"query": { "term": { "system.current": true } }"""
    else
      """"query": { "match_all": {} }"""

  /** A comma-delimited list of field names that are to be fetched from ES */
  def includeFields: String

  /** An estimate of the size of one infoton's worth of JSON data, as returned from ES */
  def infotonSize: Int
}


