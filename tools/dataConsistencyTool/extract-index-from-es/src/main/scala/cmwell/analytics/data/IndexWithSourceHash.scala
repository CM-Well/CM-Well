package cmwell.analytics.data

import java.security.MessageDigest

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.ConfigFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

case class IndexWithSourceHash(uuid: String, hash: Array[Byte]) extends GenericRecord {

  // This doesn't actually use any of the Avro serialization framework - none of these are used.
  override def put(key: String, v: scala.Any): Unit = ???

  override def get(key: String): AnyRef = ???

  override def put(i: Int, v: scala.Any): Unit = ???

  override def get(i: Int): AnyRef = ???

  override def getSchema: Schema = ???
}

object IndexWithSourceHash extends ObjectExtractor[IndexWithSourceHash] {

  val schema: Schema = SchemaBuilder
    .record("IndexWithSourceHash").namespace("cmwell.analytics")
    .fields
    .name("uuid").`type`.stringType.noDefault
    .name("hash").`type`.stringType.noDefault
    .endRecord

  private val config = ConfigFactory.load
  val infotonSize: Int = config.getInt("extract-index-from-es.fetch-size-index-with-system-fields")

  def includeFields: String = {
    throw new NotImplementedException() // Doesn't make sense to use source filtering here
  }

  private val om = new ObjectMapper()
  private val digest = MessageDigest.getInstance("MD5")

  def extractFromJson(hit: JsonNode): IndexWithSourceHash = {

    val source = hit.findValue("_source")
    digest.reset()
    digest.update(om.writeValueAsBytes(source))

    IndexWithSourceHash(
      uuid = hit.findValue("_id").asText,
      hash = digest.digest)
  }
}