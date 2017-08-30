package cmwell.dc

import com.typesafe.scalalogging.{LazyLogging => LazyLoggingWithoutRed}
import org.slf4j.LoggerFactory

trait LazyLogging extends LazyLoggingWithoutRed {
  protected lazy val redlog = LoggerFactory.getLogger("dc_red_log")
  protected lazy val yellowlog = LoggerFactory.getLogger("dc_yellow_log")
}