package cmwell.analytics.util

import scala.concurrent.duration.Duration

object ConsistencyThreshold {

  /**
    * The default consistency threshold for an analysis, if one is not supplied by the caller.y
    * If the lastModified for an inconsistent infoton falls on or after this time, then it is ignored.
    * This gives CM-Well some time for an infoton to reach consistency.
    *
    * This default should probably not ever be used for analysis where extracts are done and then they are
    * analyzed, since the extracts are done serially. In this case, the caller should calculate an instant
    * relative to when the extracts start and pass it as the --consistency-threshold parameter.
    */
  def defaultConsistencyThreshold: Long = System.currentTimeMillis - Duration("10 min").toMillis
}
