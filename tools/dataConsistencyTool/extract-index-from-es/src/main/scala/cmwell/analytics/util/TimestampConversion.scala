package cmwell.analytics.util

import org.joda.time.format.ISODateTimeFormat
import org.rogach.scallop.{ValueConverter, singleArgConverter}


object TimestampConversion {

  def convertToTimestamp(timestampString: String): java.sql.Timestamp = {
    val dateTime = ISODateTimeFormat.dateTime.parseDateTime(timestampString)
    new java.sql.Timestamp(dateTime.getMillis)
  }

  def convertToString(timestamp: java.sql.Timestamp): String =
    ISODateTimeFormat.dateTime.print(timestamp.getTime)

  val timestampConverter: ValueConverter[java.sql.Timestamp] = singleArgConverter[java.sql.Timestamp](convertToTimestamp)
}
