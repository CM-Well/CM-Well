package cmwell.tools.data

import cmwell.tools.data.downloader.consumer.Downloader.Token
import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats

package object sparql {
  type TokenAndStatisticsMap = Map[String, TokenAndStatistics]
  type TokenAndStatistics = (Token,Option[DownloadStats])
}
