package cmwell.analytics.util

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.substring

case class DatasetFilter(lastModifiedGte: Option[java.sql.Timestamp] = None,
                         current: Option[Boolean] = None,
                         pathPrefix: Option[String] = None) {

  def applyFilter[T](ds: Dataset[T],
                     forAnalysis: Boolean): Dataset[T] = {

    // Apply filters if a filter value was supplied, and the corresponding column exists in ds

    val lastModifiedOptionalPredicate: Option[Column] =
      if (ds.columns.contains("lastModified"))
        lastModifiedGte.map(ds("lastModified") >= _)
      else
        None

    val currentOptionalPredicate =
      if (ds.columns.contains("current"))
        current.map(ds("current") > _)
      else
        None

    val pathOptionalPredicate =
      if (ds.columns.contains("path"))
        pathPrefix.map { pathPrefix => substring(ds("path"), 0, pathPrefix.length) === pathPrefix }
      else
        None

    val temporalOptionalPredicate = (lastModifiedOptionalPredicate, currentOptionalPredicate) match {
      case (Some(lastModifiedPredicate), Some(currentPredicate)) if forAnalysis =>
        Some(lastModifiedPredicate && currentPredicate)
      case (Some(lastModifiedPredicate), Some(currentPredicate)) if !forAnalysis =>
        Some(lastModifiedPredicate || currentPredicate)
      case (Some(lastModifiedPredicate), _) =>
        Some(lastModifiedPredicate)
      case (_, Some(currentPredicate)) =>
        Some(currentPredicate)
      case _ =>
        None
    }

    val overallOptionalPredicate = (pathOptionalPredicate, temporalOptionalPredicate) match {
      case (Some(pathPredicate), Some(currentPredicate)) =>
        Some(pathPredicate && currentPredicate)
      case (Some(pathPredicate), _) =>
        Some(pathPredicate)
      case (_, Some(temporalPredicate)) =>
        Some(temporalPredicate)
      case _ =>
        None
    }

    overallOptionalPredicate.fold(ds)(ds.filter)
  }
}

