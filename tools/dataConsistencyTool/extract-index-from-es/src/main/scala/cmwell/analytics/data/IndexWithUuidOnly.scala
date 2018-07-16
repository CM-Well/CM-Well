package cmwell.analytics.data

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}

case class IndexWithUuidOnly(uuid: String) extends GenericRecord with CsvGenerator{
  
  override def put(key: String, v: scala.Any): Unit = ???

  override def get(key: String): AnyRef = key match {
    case "uuid" => uuid
    case _ => throw new IllegalArgumentException
  }

  override def put(i: Int, v: scala.Any): Unit = ???

  override def get(i: Int): AnyRef = i match {
    case 0 => uuid
    case _ => throw new IllegalArgumentException
  }

  override def getSchema: Schema = IndexWithUuidOnly.schema

  override def csv: String = uuid
}

object IndexWithUuidOnly extends ObjectExtractor[IndexWithUuidOnly] {

  val schema: Schema = SchemaBuilder
    .record("cmwell.analytics.data.IndexWithUuidOnly").namespace("cmwell.analytics")
    .fields
    .name("uuid").`type`.stringType.noDefault
    .endRecord

  private val config = ConfigFactory.load
  val infotonSize: Int = config.getInt("extract-index-from-es.fetch-size-index-with-uuid-only")

  val includeFields: String = s""""_source": ["xxx"]""" // There is no such field - we don't want any!

  def extractFromJson(hit: JsonNode): IndexWithUuidOnly =
    IndexWithUuidOnly(uuid = hit.findValue("_id").asText)
}