package cmwell.analytics.data

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths, StandardOpenOption}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.util.ByteString
import cmwell.analytics.util.HttpUtil
import com.typesafe.config.ConfigFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

trait DataWriter[T <: GenericRecord] {

  def write(record: T): Unit

  def close(): Unit
}

/**
  * Allows for writing data either as csv or parquet.
  */
trait FileDataWriter[T <: GenericRecord] extends DataWriter[T] {

  def fileExtension: String
}

object FileDataWriter {

  def apply[T <: GenericRecord with CsvGenerator](format: String,
                                                  schema: Schema,
                                                  path: String,
                                                  compressionCodecName: CompressionCodecName = CompressionCodecName.SNAPPY
                                                 ): FileDataWriter[T] = format match {
    case "parquet" => new ParquetDataWriter[T](schema, path, compressionCodecName)
    case "csv" => new CsvDataWriter[T](schema, path)
    case _ => throw new IllegalArgumentException
  }
}

class ParquetDataWriter[T <: GenericRecord](schema: Schema,
                                            path: String,
                                            compressionCodecName: CompressionCodecName) extends FileDataWriter[T] {

  private val parquetWriter: ParquetWriter[GenericRecord] = AvroParquetWriter.builder[GenericRecord](new Path(path))
    .withSchema(schema)
    .withCompressionCodec(compressionCodecName)
    .build

  override def fileExtension: String = ".parquet"

  override def write(record: T): Unit = parquetWriter.write(record)

  override def close(): Unit = parquetWriter.close()
}

object CsvDataWriter {
  private val config = ConfigFactory.load
  private val CsvChunkSize = config.getInt("extract-index-from-es.csv-write-chunk-size")
}

class CsvDataWriter[T <: GenericRecord with CsvGenerator](schema: Schema, path: String) extends FileDataWriter[T] {

  private val buffer = new ByteArrayOutputStream(CsvDataWriter.CsvChunkSize)
  private val newline = "\n".getBytes(UTF_8)

  override def fileExtension: String = ".csv"

  override def write(record: T): Unit = {
    val data = record.csv.getBytes(UTF_8)

    if ((buffer.size + data.length + 1) > CsvDataWriter.CsvChunkSize)
      flush()

    buffer.write(data)
    buffer.write(newline)
  }

  override def close(): Unit = flush()

  private def flush(): Unit = {
    if (buffer.size > 0) {
      Files.write(Paths.get(path), buffer.toByteArray, StandardOpenOption.APPEND)
      buffer.reset()
    }
  }
}

object IndexDataWriter {
  private val config = ConfigFactory.load
  private val chunkSize = config.getInt("extract-index-from-es.bulk-index-chunk-size")
  private val writeTimeout = FiniteDuration(config.getDuration("extract-index-from-es.bulk-index-write-timeout").toMillis, MILLISECONDS)
}

class IndexDataWriter[T <: GenericRecord](indexName: String,
                                          esEndpoint: String)
                                         (implicit system: ActorSystem,
                                          executionContext: ExecutionContextExecutor,
                                          actorMaterializer: ActorMaterializer
                                         ) extends DataWriter[T] {

  private val logger = LogManager.getLogger(IndexDataWriter.getClass)

  private var count: Int = 0

  private var bufferedCount: Int = 0
  private var buffer = ByteString.empty

  override def write(record: T): Unit = {

    if (bufferedCount >= IndexDataWriter.chunkSize)
      flush()

    val id = record.get("uuid")
    val document = record.get("document").toString

    // _index and _type are omitted from the index action - they are specified once in the URL.
    // The action_and_meta_data and optional_source must both end with a newline.
    val actionAndMetaAndSource =
    s"""{"index":{"_id":"$id"}}\n$document\n"""

    buffer = buffer ++ ByteString(actionAndMetaAndSource.getBytes(UTF_8))
    bufferedCount += 1
  }

  override def close(): Unit = {
    flush()
    logger.info(s"$count documents were written to _bulk API.")
  }

  private def flush(): Unit = {

    if (bufferedCount > 0) {
      logger.debug(s"Flushing $bufferedCount documents to _bulk API.")

      val url = s"http://$esEndpoint/$indexName/infoclone/_bulk"
      val request = HttpRequest(method = POST, uri = url, entity = buffer)

      // TODO: Add retry logic
      val response = HttpUtil.jsonResult(request, s"Bulk index: $url", timeout = IndexDataWriter.writeTimeout)

      val failedUuids = response.get("items").iterator.asScala
        .filter { item =>
          val statusCode = item.get("index").get("status").asInt
          statusCode < 200 || statusCode > 299
        }
        .map(item => item.get("index").get("_id").asText)
        .toVector

      if (response.get("errors").asBoolean || failedUuids.nonEmpty) {

        val msg = s"""Indexing failed for _ids: ${failedUuids.mkString(",")}"""
        logger.error(msg)
        throw new RuntimeException(msg)
      }

      count += bufferedCount
      buffer = ByteString.empty
      bufferedCount = 0
    }
  }
}

/**
  * HashSummarize takes the hash of each document in the shard, and summarizes them using XOR.
  * The XOR function works here since the order of XOR operations is not significant.
  * This property also allows the XOR summaries to be combined across shards into a single XOR summary
  * for an entire index.
  *
  * The hash is an MD5 hash, and is always 128 bits.
  */
case class XORSummary(index: String,
                      shard: String,
                      summary: Array[Byte] = Array.ofDim[Byte](128 / 8)) extends DataWriter[IndexWithSourceHash] {

  override def write(record: IndexWithSourceHash): Unit = {
    for (i <- summary.indices)
      summary(i) = (summary(i) ^ record.hash(i)).toByte
  }

  override def close(): Unit = {}
}

object XORSummary {

  val SummaryLengthInBytes: Int = 128 / 8

  def combine(summary1: XORSummary, summary2: XORSummary): XORSummary = {

    assert(summary1.summary.length == SummaryLengthInBytes)
    assert(summary2.summary.length == SummaryLengthInBytes)
    val combinedSummary = Array.ofDim[Byte](SummaryLengthInBytes)

    for (i <- 0 until SummaryLengthInBytes)
      combinedSummary(i) = (summary1.summary(i) ^ summary2.summary(i)).toByte

    XORSummary(
      index = if (summary1.index == summary2.index) summary1.index else "*",
      shard = "*",
      combinedSummary)
  }
}