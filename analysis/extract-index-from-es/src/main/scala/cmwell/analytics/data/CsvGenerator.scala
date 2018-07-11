package cmwell.analytics.data

/**
  * This expresses our limited CSV production capabilities for our data objects that extend GenericRecord.
  * CSV loses null information for some types, and can't represent structured types.
  * For some cases (e.g., uuids only), it is useful and trivial to implement.
  */
trait CsvGenerator {
  def csv: String = ???
}