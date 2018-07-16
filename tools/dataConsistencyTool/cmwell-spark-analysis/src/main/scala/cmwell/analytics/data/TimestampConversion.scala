package cmwell.analytics.data

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.to_timestamp

object TimestampConversion {

  private val ISO8601Format = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

  // This conversion truncates millisecond precision.
  // If full precision is required, we may need to implement a UDF.
  def convertISO8601ToDate(column: Column): Column =
    to_timestamp(column, ISO8601Format)
}
