package cmwell.analytics.util

object ISO8601 {

  def instantToMillis(instant: String): Long = java.time.Instant.parse(instant).toEpochMilli

  def instantToText(instant: Long): String = java.time.Instant.ofEpochMilli(instant).toString
}
