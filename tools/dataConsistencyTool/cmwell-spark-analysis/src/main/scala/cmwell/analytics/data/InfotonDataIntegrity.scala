package cmwell.analytics.data

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import cmwell.analytics.util.CassandraSystem
import com.datastax.spark.connector._
import com.google.common.primitives.Longs
import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang.StringUtils.isNotEmpty
import org.apache.spark.sql.{Dataset, SparkSession}
import org.joda.time.format.ISODateTimeFormat

import scala.collection.breakOut
import scala.util.Try
import scala.util.control.NonFatal

/**
  * This representation of an Infoton includes all the fields and data that is necessary to calculate the uuid.
  *
  * This will detect infotons that have missing or corrupted data. In addition to checking that the uuid can be
  * recalculated correctly, some basic checks are done on the infoton since some kinds of incorrect data
  * (esp. duplicated fields) might not be ignored in the hash calculation.
  *
  * The Dataset returned includes all the data needed to determine that the infoton has data that agrees with
  * the uuid, as well as some Boolean columns that indicate possible reasons for the failure.
  */

case class DataChunk(index: String, data: Array[Byte])

case class Field(name: String, values: Seq[String])

case class InfotonDataIntegrity(`type`: String,
                                uuid: String,
                                lastModified: String,
                                path: String,
                                fields: Seq[Field],
                                data: List[DataChunk],
                                contentPointer: String,
                                mimeType: String,
                                linkTo: String,
                                linkType: String,
                                contentLength: String,

                                hasIncorrectUuid: Boolean,
                                hasMissingOrIllFormedSystemFields: Boolean,
                                hasDuplicatedSystemFields: Boolean,
                                hasInvalidContent: Boolean,
                                hasUnknownSystemField: Boolean)

object InfotonDataIntegrity extends EstimateDatasetSize {

  private val BytesPerRow = 16 + (16 * 8) + 1 + 32 + 24 + 32 + 256

  override def estimateDatasetSize(implicit spark: SparkSession): Long =
    CassandraSystem.rowCount(table = "infoton") * BytesPerRow

  /**
    * Get a Dataset[InfotonDataIntegrity].
    */
  def apply()
           (implicit spark: SparkSession): Dataset[InfotonDataIntegrity] = {

    val infotonRdd = spark.sparkContext.cassandraTable("data2", "infoton")
      // This was an attempt to use a reasonable number of partitions (as opposed to 10K).
      // It made about that many partitions, but reduced the parallelism level to 4.
      //.withReadConf(ReadConf(splitCount = Some(1000)))  // attempt to force using a reasonable number of partitions
      .select("uuid", "quad", "field", "value", "data")
      .spanBy(row => row.getString("uuid"))

    // Map the grouped data to Infoton objects containing only the system fields.
    val objectRDD = infotonRdd.map { case (uuid, rows) =>

      // Test for duplicated field names.
      // 'data' is excluded since multiple data chunks can be present.
      val fieldNames: Seq[String] = rows.filter(row =>
        row.getString("field") != "data" && // data can legitimately be duplicated
          row.getString("quad") == "cmwell://meta/sys") // only look at system fields
        .map(row => row.getString("field"))(breakOut)

      val hasDuplicatedSystemFields = fieldNames.length != fieldNames.distinct.length

      var `type`: String = null
      var lastModified: String = null
      var path: String = null

      var fields = Map.empty[String, Set[String]]

      // Note that CM-Well is implemented with an inherent limit on the length of content of Int.MaxValue
      var data = List.empty[DataChunk]
      var contentPointer: String = null
      var mimeType: String = null
      var contentLength: String = null

      var linkTo: String = null
      var linkType: String = null

      var unknownSystemField = false

      rows.foreach { infotonRow =>

        if (infotonRow.getString("quad") == "cmwell://meta/sys") {

          infotonRow.getString("field") match {
            case "type" => `type` = infotonRow.getString("value")
            case "lastModified" => lastModified = infotonRow.getString("value")
            case "path" => path = infotonRow.getString("value")

            case "contentPointer" => contentPointer = infotonRow.getString("value")
            case "contentLength" => contentLength = infotonRow.getString("value")
            case "mimeType" => mimeType = infotonRow.getString("value")
            case "data" =>
              val index = infotonRow.getString("value")
              val bytes = infotonRow.getBytes("data").array
              data = DataChunk(index, bytes) :: data

            case "linkTo" => linkTo = infotonRow.getString("value")
            case "linkType" => linkType = infotonRow.getString("value")

            case "dc" | "indexName" | "indexTime" => // Known system fields that are not included in the hash

            case _ => unknownSystemField = true
          }

        } else {
          val fieldNameWithTypePrefix = infotonRow.getString("field")
          val fieldName = fieldNameWithTypePrefix.drop(fieldNameWithTypePrefix.indexOf('$') + 1)
          val value = infotonRow.getString("value")

          // There can be multiple values for each field name.
          // The values for a field are treated as a set, so multiple occurrences of the same value is treated
          // the same as a single value.
          val newValues = fields.get(fieldName).fold(Set(value))(_ + value)
          fields = fields + (fieldName -> newValues)
        }
      }

      val isContentValid = `type` match {

        case "f" => isFileInfotonContentValid(data = data,
          contentLength = contentLength,
          contentPointer = contentPointer,
          mimeType = mimeType)

        case "l" => isLinkInfotonContentValid(linkTo = linkTo, linkType = linkType)

        case _ => // Don't expect content with these types - check that there is none.
          contentLength == null && contentPointer == null &&
            data.isEmpty &&
            linkTo == null && linkType == null
      }

      val fieldData: Seq[Field] = fields.map { case (name, values) => Field(name, values.toSeq) }(breakOut)

      // Recalculate the uuid and check it against the stored value.
      // This is done in a Try to prevent missing (i.e., null) fields from causing analysis to fail.
      // If calculating the uuid fails, it is presumed that the reason is ill-formed system fields.

      // The hash is done over the string value for each field.
      // In the Infoton code, the values are converted to their type, but then converted toString when hashed.
      // Here, we just leave the values as Strings.

      val hasIncorrectUuid: Try[Boolean] = Try(uuid != calculateUuid(
        lastModified = lastModified,
        `type` = `type`,
        data = data,
        mimeType = mimeType,
        contentPointer = contentPointer,
        linkTo = linkTo,
        linkType = linkType,
        path = path,
        fields = fieldData))

      InfotonDataIntegrity(
        `type` = `type`,
        uuid = uuid,
        lastModified = lastModified,
        path = path,
        fields = fieldData,
        data = data,
        contentPointer = contentPointer,
        mimeType = mimeType,
        linkTo = linkTo,
        linkType = linkType,
        contentLength = contentLength,

        hasIncorrectUuid = hasIncorrectUuid.getOrElse(false),
        hasMissingOrIllFormedSystemFields = hasIncorrectUuid.isFailure,
        hasDuplicatedSystemFields = hasDuplicatedSystemFields,
        hasInvalidContent = !isContentValid,
        hasUnknownSystemField = unknownSystemField)
    }

    import spark.implicits._
    spark.createDataset(objectRDD)
  }

  def calculateUuid(infoton: InfotonDataIntegrity): String =
    calculateUuid(
      lastModified = infoton.lastModified,
      `type` = infoton.`type`,
      data = infoton.data,
      mimeType = infoton.mimeType,
      contentPointer = infoton.contentPointer,
      linkTo = infoton.linkTo,
      linkType = infoton.linkType,
      path = infoton.path,
      fields = infoton.fields)

  def calculateUuid(lastModified: String,
                    `type`: String,
                    data: List[DataChunk],
                    mimeType: String,
                    contentPointer: String,
                    linkTo: String,
                    linkType: String,
                    path: String,
                    fields: Seq[Field]
                   ): String = {

    // These get methods convert the value to the typed value.
    // If the conversion fails, the exception will cause the uuid calculation to fail.

    def lastModifiedMillis: java.lang.Long = java.lang.Long.valueOf(
      ISODateTimeFormat.dateTime.parseDateTime(lastModified).getMillis)

    // This does not check that the data is correctly constructed, but it will produce the correct hash if
    // correct data is supplied. Duplicated system fields could result in correct or incorrect hashes, depending
    // on the order they are used, so separate checks for duplicate fields and data integrity are required.
    def extraBytes: List[Array[Byte]] = `type` match {

      case "f" => // FileInfoton

        def mimeTypeBytes: Array[Byte] = if (isNotEmpty(mimeType)) mimeType.getBytes(UTF_8) else Array.empty[Byte]

        def dataLength: Long = (0L /: data) (_ + _.data.length)

        def dataBytes: List[Array[Byte]] = data.sortBy(_.index.toInt).map(_.data)

        if (contentPointer != null)
          mimeTypeBytes :: contentPointer.getBytes(UTF_8) :: Nil
        else if (data.nonEmpty)
          mimeTypeBytes :: Longs.toByteArray(dataLength) :: dataBytes
        else
          mimeTypeBytes :: Nil

      case "l" => // LinkInfoton

        val linkToBytes = if (isNotEmpty(linkTo)) linkTo.getBytes(UTF_8) else Array.empty[Byte]

        val linkTypeBytes = if (isNotEmpty(linkType)) linkType.getBytes(UTF_8) else Array.empty[Byte]

        linkToBytes :: linkTypeBytes :: Nil

      case _ => // Any other type doesn't have data associated with it.
        Nil
    }

    val digest = MessageDigest.getInstance("MD5")

    def updateDigestFromString(s: String): Unit = digest.update(s.getBytes(UTF_8))

    def updateDigestFromLong(l: Long): Unit = digest.update(Longs.toByteArray(l))

    // Calculate the uuid hash

    updateDigestFromString(path)
    updateDigestFromLong(lastModifiedMillis)

    fields.sortBy(_.name).foreach { field =>
      updateDigestFromString(field.name)
      field.values.sorted.foreach(updateDigestFromString)
    }

    extraBytes.foreach(digest.update)

    Hex.encodeHexString(digest.digest())
  }

  /** Validate that the binary data content is valid:
    * - Data chunk indexes convert to integer type.
    * - No duplicated chunks.
    * - Data is complete.
    * - contentLength field is consistent with data size.
    */
  def isFileInfotonContentValid(data: Seq[DataChunk],
                                contentLength: String,
                                contentPointer: String,
                                mimeType: String): Boolean = {

    val isMimeTypeValid = isNotEmpty(mimeType)

    val isContentValid = try {
      if (contentPointer != null) {

        // The content is external, and all we can do here is to validate the content length
        // and ensure that a content pointer was provided.
        val storedContentLength = contentLength.toLong
        storedContentLength >= 0 && contentPointer.nonEmpty
      }
      else if (data.nonEmpty) {
        // One or more chunks of data are defined.

        // The indexes of the data chunks must be consecutive indexes starting at 0.
        val indexes = data.map(_.index.toInt).sorted
        val indexesAreComplete = indexes == indexes.indices

        // The value of the contentLength system field must equal the total length of the data actually stored.
        val storedContentLength = contentLength.toLong
        val calculatedContentLength = (0L /: data) (_ + _.data.length)
        val storedContentLengthMatchesActualContentLength = storedContentLength == calculatedContentLength

        indexesAreComplete && storedContentLengthMatchesActualContentLength
      }
      else {
        // No content is also valid
        true
      }
    }
    catch {
      case NonFatal(_) => false // Something failed conversion
    }

    isMimeTypeValid && isContentValid
  }

  def isLinkInfotonContentValid(linkTo: String, linkType: String): Boolean =
    isNotEmpty(linkTo) && isNotEmpty(linkType)
}
