package cmwell.analytics.util

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Takes two Datasets that each contain a single 'uuid' column, and does a set difference on the two datasets
  * (i.e., determine the uuids in uuids1 that are not also in uuids2). The resulting uuids are expanded to include
  * the uuid, path and lastModified. A filter is applied to remove any rows that are "current" since we allow them
  * to be inconsistent for a short time.
  *
  * This implicitly brings the entire set difference result to the driver. This should normally be fine since we
  * expect the number of inconsistent infotons to be low enough to allow it. In general, this creates a possibility
  * for failure the number of inconsistencies is very large.
  *
  * Bringing all the data locally also means that writing the data out would result in a single partition file
  * being created, which is most likely what we are after anyway.
  *
  * When doing differences between systems, positives for the root path ("/") and paths starting with "/meta/" are
  * filtered out, since those infotons are created specifically for a CM-Well instance, and will not have the same
  * uuids between systems.
  */
object SetDifferenceAndFilter {

  def apply(uuids1: Dataset[KeyFields],
            uuids2: Dataset[KeyFields],
            consistencyThreshold: Long,
            filterOutMeta: Boolean = false)
           (implicit spark: SparkSession): Dataset[KeyFields] = {

    import spark.implicits._

    // The original setDifference implementation used the SQL except function, but that ignores any pre-partitioning.
    // The next implementation used a left-anti join, but that created a weird execution plan that caused poor performance.
    // The current implementation uses a outer join - which uses an efficient sort-merge join.

    def setDifference(uuids1: Dataset[KeyFields], uuids2: Dataset[KeyFields]): Dataset[KeyFields] =
      uuids1.join(uuids2, uuids1("uuid") === uuids2("uuid"), "left_outer")
        .filter(uuids2("uuid").isNull)
        .select(uuids1("*"))
        .as[KeyFields]

    // Calculate the set difference between the two sets of uuids.
    // The anti-join produces just the left side, and only the ones that are not in the right side.
    val positives = setDifference(uuids1, uuids2)

    val timeToConsistencyFilter = positives("lastModified") < new Timestamp(consistencyThreshold)
    val overallFilter = if (filterOutMeta)
      timeToConsistencyFilter &&
        (positives("path") =!= "/" && positives("path") =!= "/meta" && !positives("path").startsWith("/meta/"))
    else
      timeToConsistencyFilter

    // Filter out any positives that occurred after the current threshold
    positives.filter(overallFilter)
  }
}
