package cmwell.analytics.data

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}

case class IndexWithCompleteDocument(uuid: String, document: String) extends GenericRecord with CsvGenerator {

  override def put(key: String, v: scala.Any): Unit = ???

  override def get(key: String): AnyRef = key match {
    case "uuid" => uuid
    case "document" => document
    case _ => throw new IllegalArgumentException
  }

  override def put(i: Int, v: scala.Any): Unit = ???

  override def get(i: Int): AnyRef = i match {
    case 0 => uuid
    case 1 => document
    case _ => throw new IllegalArgumentException
  }

  override def getSchema: Schema = IndexWithCompleteDocument.schema

  // Specifically don't implement CsvGenerator.csv since it is guaranteed to be invalid CSV - force use of Parquet.
}

object IndexWithCompleteDocument extends ObjectExtractor[IndexWithCompleteDocument] {

  val schema: Schema = SchemaBuilder
    .record("IndexWithCompleteDocument").namespace("cmwell.analytics")
    .fields
    .name("uuid").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .name("document").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .endRecord

  private val config = ConfigFactory.load
  val infotonSize: Int = config.getInt("extract-index-from-es.fetch-size-index-with-complete-document")

  def includeFields: String = s""""_source": "*""""

  def extractFromJson(hit: JsonNode): IndexWithCompleteDocument =
    IndexWithCompleteDocument(
      uuid = hit.findValue("_id").asText,
      document = hit.findValue("_source").toString)
}