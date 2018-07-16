package cmwell.analytics.data

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigFactory
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.LogManager
import org.joda.time.format.ISODateTimeFormat

import scala.util.control.NonFatal


case class IndexWithKeyFields(uuid: String,
                              lastModified: java.sql.Timestamp,
                              path: String) extends GenericRecord with CsvGenerator {

  override def put(key: String, v: scala.Any): Unit = ???

  override def get(key: String): AnyRef = key match {
    case "uuid" => uuid
    case "lastModified" => java.lang.Long.valueOf(lastModified.getTime)
    case "path" => path
  }

  override def put(i: Int, v: scala.Any): Unit = ???

  override def get(i: Int): AnyRef = i match {
    case 0 => uuid
    case 1 => java.lang.Long.valueOf(lastModified.getTime)
    case 2 => path
    case _ => throw new IllegalArgumentException
  }

  override def getSchema: Schema = IndexWithSystemFields.schema

  override def csv: String =
    (if (uuid == null) "" else uuid) + "," +
      (if (lastModified == null) "" else ISODateTimeFormat.dateTime.print(lastModified.getTime)) + "," +
      (if (path == null) "" else path)
}

object IndexWithKeyFields extends ObjectExtractor[IndexWithKeyFields] {

  private val logger = LogManager.getLogger(IndexWithSystemFields.getClass)

  // AVRO-2065 - doesn't allow union over logical type, so we can't make timestamp column nullable.
  val timestampMilliType: Schema = LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG))

  val schema: Schema = SchemaBuilder
    .record("IndexWithSystemFields").namespace("cmwell.analytics")
    .fields
    .name("uuid").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .name("lastModified").`type`(timestampMilliType).noDefault
    .name("path").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .endRecord

  private val config = ConfigFactory.load
  val infotonSize: Int = config.getInt("extract-index-from-es.fetch-size-index-with-uuid-lastModified-path")

  def includeFields: String = {
    // Note that 'quad' is not included in this list
    val fields = "uuid,lastModified,path"
      .split(",")
      .map(name => s""""system.$name"""")
      .mkString(",")

    s""""_source": [$fields]"""
  }

  def extractFromJson(hit: JsonNode): IndexWithKeyFields = {

    val system = hit.findValue("_source").findValue("system")

    def extractString(name: String): String = system.findValue(name) match {
      case x: JsonNode => x.asText
      case _ => null
    }

    // Extracting date values as Long - as a java.sql.Date might be better
    def extractDate(name: String): java.sql.Timestamp = system.findValue(name) match {
      case x: JsonNode =>
        try {
          new java.sql.Timestamp(ISODateTimeFormat.dateTime.parseDateTime(x.asText).getMillis)
        }
        catch {
          case NonFatal(ex) =>
            logger.warn(s"Failed conversion of date value: $x", ex)
            throw ex
        }
      case _ => null
    }

    IndexWithKeyFields(
      uuid = extractString("uuid"),
      lastModified = extractDate("lastModified"),
      path = extractString("path"))
  }
}