package cmwell.analytics.data

import java.io.File
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cmwell.analytics.util.Shard
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.FileUtils
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.ExecutionContextExecutor

trait DataWriterFactory[T <: GenericRecord] {
  def apply(shard: Shard): DataWriter[T]
}

object DataWriterFactory {

  private val compressionCodec = CompressionCodecName.SNAPPY


  def file[T <: GenericRecord with CsvGenerator](format: String,
                                                 objectExtractor: ObjectExtractor[T],
                                                 outDirectory: String): Shard => DataWriter[T] = {

    val extension = s".$format" + (if (format == "parquet") s"${compressionCodec.getExtension}" else "")

    // Generate a meaningful file name for the target file name based on the source shard index name and shard number.
    (sourceShard: Shard) => {
      val outFile: File = Paths.get(outDirectory, s"part-r-${sourceShard.indexName}.${sourceShard.shard}$extension").toFile

      if (outFile.exists)
        FileUtils.forceDelete(outFile)

      new File(outFile.getParent).mkdirs()

      FileDataWriter[T](format, objectExtractor.schema, outFile.toString, compressionCodec)
    }
  }

  /** Write to a given index name */
  def index[T <: GenericRecord](indexName: String,
                                esEndpoint: String)
                               (implicit system: ActorSystem,
                                executionContext: ExecutionContextExecutor,
                                actorMaterializer: ActorMaterializer
                               ): Shard => DataWriter[T] = {

    (_: Shard) => new IndexDataWriter[T](indexName = indexName, esEndpoint = esEndpoint)
  }

  /** Write to a given index by mapping the source index name to target index name. */
  def index[T <: GenericRecord](indexMap: Map[String, String], // source-index -> target-index
                                esEndpoint: String)
                               (implicit system: ActorSystem,
                                executionContext: ExecutionContextExecutor,
                                actorMaterializer: ActorMaterializer
                               ): Shard => DataWriter[T] = {

    (sourceShard: Shard) => {
      val targetIndex = indexMap(sourceShard.indexName)
      new IndexDataWriter[T](indexName = targetIndex, esEndpoint = esEndpoint)
    }
  }
}
