package cmwell.analytics.data

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.log4j.LogManager
import org.joda.time.format.ISODateTimeFormat

import scala.util.control.NonFatal


case class IndexWithSystemFields(kind: String,
                                 uuid: String,
                                 lastModified: java.lang.Long,
                                 path: String,
                                 dc: String,
                                 indexName: String,
                                 indexTime: java.lang.Long,
                                 parent: String,
                                 current: java.lang.Boolean) extends GenericRecord with CsvGenerator {

  override def put(key: String, v: scala.Any): Unit = ???

  override def get(key: String): AnyRef = key match {
    case "kind" => kind
    case "uuid" => uuid
    case "lastModified" => lastModified
    case "path" => path
    case "dc" => dc
    case "indexName" => indexName
    case "indexTime" => indexTime
    case "parent" => parent
    case "current" => current
    case _ => throw new IllegalArgumentException
  }

  override def put(i: Int, v: scala.Any): Unit = ???

  override def get(i: Int): AnyRef = i match {
    case 0 => kind
    case 1 => uuid
    case 2 => lastModified
    case 3 => path
    case 4 => dc
    case 5 => indexName
    case 6 => indexTime
    case 7 => parent
    case 8 => current
    case _ => throw new IllegalArgumentException
  }

  override def getSchema: Schema = IndexWithSystemFields.schema

  override def csv: String =
    (if (kind == null) "" else kind) + "," +
      (if (uuid == null) "" else uuid) + "," +
      (if (lastModified == null) "" else lastModified.toString) + "," +
      (if (path == null) "" else path) + "," +
      (if (dc == null) "" else dc) + "," +
      (if (indexName == null) "" else indexName) + "," +
      (if (indexTime == null) "" else indexTime.toString) + "," +
      (if (parent == null) "" else parent) + "," +
      (if (current == null) "" else current.toString)
}

object IndexWithSystemFields extends ObjectExtractor[IndexWithSystemFields] {

  private val logger = LogManager.getLogger(IndexWithSystemFields.getClass)

  val schema: Schema = SchemaBuilder
    .record("IndexWithSystemFields").namespace("cmwell.analytics")
    .fields
    .name("kind").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .name("uuid").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .name("lastModified").`type`.unionOf.longType.and.nullType.endUnion.noDefault
    .name("path").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .name("dc").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .name("indexName").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .name("indexTime").`type`.unionOf.longType.and.nullType.endUnion.noDefault
    .name("parent").`type`.unionOf.stringType.and.nullType.endUnion.noDefault
    .name("current").`type`.unionOf.booleanType.and.nullType.endUnion.noDefault
    .endRecord

  private val config = ConfigFactory.load
  val infotonSize: Int = config.getInt("extract-index-from-es.fetch-size-index-with-system-fields")

  def includeFields: String = {
    // Note that 'quad' is not included in this list
    // uuid and indexName are extracted from _id and _index
    val fields = "kind,lastModified,path,dc,indexTime,parent,current"
      .split(",")
      .map(name => s""""system.$name"""")
      .mkString(",")

    s""""_source": [$fields]"""
  }

  def extractFromJson(hit: JsonNode): IndexWithSystemFields = {

    val system = hit.findValue("_source").findValue("system")

    def extractMetaString(name: String): String = hit.findValue(name) match {
      case x: JsonNode => x.asText
      case _ => null  // should never get here - meta fields should always be populated.
    }

    def extractString(name: String): String = system.findValue(name) match {
      case x: JsonNode => x.asText
      case _ => null
    }

    // Extracting date values as Long - as a java.sql.Date might be better
    def extractDate(name: String): java.lang.Long = system.findValue(name) match {
      case x: JsonNode =>
        try {
          ISODateTimeFormat.dateTime.parseDateTime(x.asText).getMillis
        }
        catch {
          case NonFatal(ex) =>
            logger.warn(s"Failed conversion of date value: $x", ex)
            throw ex
        }
      case _ => null
    }

    def extractLong(name: String): java.lang.Long = system.findValue(name) match {
      case x: JsonNode =>
        try {
          x.asLong
        }
        catch {
          case NonFatal(ex) =>
            logger.warn(s"Failed conversion of long value: $x", ex)
            throw ex
        }
      case _ => null
    }

    def extractBoolean(name: String): java.lang.Boolean = system.findValue(name) match {
      case x: JsonNode =>
        try {
          x.asBoolean
        }
        catch {
          case NonFatal(ex) =>
            logger.warn(s"Failed conversion of boolean value: $x", ex)
            throw ex
        }
      case _ => null
    }

    IndexWithSystemFields(
      kind = extractString("kind"),
      uuid = extractMetaString("_id"),
      lastModified = extractDate("lastModified"),
      path = extractString("path"),
      dc = extractString("dc"),
      indexName = extractMetaString("_index"),
      indexTime = extractLong("indexTime"),
      parent = extractString("parent"),
      current = extractBoolean("current"))
  }
}