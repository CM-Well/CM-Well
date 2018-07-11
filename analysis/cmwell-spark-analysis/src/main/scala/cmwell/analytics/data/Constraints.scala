package cmwell.analytics.data

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}

object Constraints {

  private val ValidUuidPattern = "([a-f0-9]{32})"

  // This definition of well-formedness requires that the UUID can be decoded to a 16-byte array via binhex.
  // This is not strictly required, but we know that CM-Well creates UUID strings this way, so a deviation from
  // that pattern indicates that the data is damaged or has been tampered with somehow.
  // We might want to decode UUIDs for optimized processing (e.g., compact representation that avoids string compares).
  def isUuidWellFormed(column: Column): Column =
    length(regexp_extract(column, ValidUuidPattern, 0)).isNotNull

  def isDcWellFormed(column: Column): Column =
    isNonEmpty(column)

  // May be null if not indexed yet. It can be well-formed without yet being consistent.
  def isIndexNameWellFormed(column: Column): Column =
    when(column.isNotNull, length(column) > 0).otherwise(true)

  // May be null if not indexed yet. It can be well-formed without yet being consistent.
  // The indexTime in C* (infoton) is stored as a String value.
  def isIndexTimeCasWellFormed(column: Column): Column =
    when(column.isNotNull, column.cast(DataTypes.LongType).isNotNull).otherwise(true)

  // lastModified is stored in Cas as an ISO8601 date
  def isLastModifiedCasWellFormed(column: Column): Column =
    TimestampConversion.convertISO8601ToDate(column).isNotNull

  // lastModified is stored in ES as a Long.
  def isLastModifiedEsWellFormed(column: Column): Column =
    column.isNotNull

  def isPathWellFormed(column: Column): Column =
    isNonEmpty(column) // TODO: Could check path normalization

  def isTypeWellFormed(column: Column): Column =
    InfotonType.isWellFormedCas(column)

  def isNonEmpty(column: Column): Column =
    length(column) > 0

  // The infoton "kind" column as stored in index.
  def isKindWellFormed(column: Column): Column =
    InfotonType.isWellFormedEs(column)

  // In ES, indexTime is stored as a Long
  def isIndexTimeEsWellFormed(column: Column): Column =
    column.isNotNull // indexTime field is a Long, so a null test is sufficient

  def isParentWellFormed(column: Column): Column =
    isNonEmpty(column) // ???

  def isCurrentWellFormed(column: Column): Column =
    column.isNotNull // current field is a Boolean, so a null test is sufficient

  // The ES timestamps are retrieved as a Long (milliseconds).
  // The conversion of the ISO8601 from C* will truncate milliseconds.
  def areTimestampsConsistent(iso8601Column: Column, millisColumn: Column): Column =
    TimestampConversion.convertISO8601ToDate(iso8601Column).cast(LongType) === (millisColumn / 1000).cast(LongType)
}
