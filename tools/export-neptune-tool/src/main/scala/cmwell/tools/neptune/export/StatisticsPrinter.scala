/**
  * Copyright 2015 Thomson Reuters
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package cmwell.tools.neptune.export

import org.apache.commons.lang3.time.DurationFormatUtils
import org.slf4j.LoggerFactory

object StatisticsPrinter {

  protected lazy val logger = LoggerFactory.getLogger("statistics_printer")
  var totalInfotons = 0L
  var bulkConsumeTotalTime = 0L
  var neptuneTotalTime = 0L

   def printBulkStatistics(readInputStreamDuration: Long, totalInfotons: String) = {
    this.totalInfotons+=totalInfotons.toLong
    bulkConsumeTotalTime+=readInputStreamDuration
    val bulkConsume = DurationFormatUtils.formatDurationWords(bulkConsumeTotalTime, true, true)
    val avgInfotonsPerSec = this.totalInfotons / (bulkConsumeTotalTime / 1000)
    val summaryLogMsg = "Total Infotons: " + this.totalInfotons.toString.padTo(15, ' ') + "Consume Duration: " + bulkConsume.padTo(25, ' ') +
      "avg Infotons/sec:" + avgInfotonsPerSec
    logger.info(summaryLogMsg)
    print(summaryLogMsg + "\r")
  }


   def printBulkStatistics(readInputStreamDuration: Long, totalInfotons: String, neptuneDurationMilis: Long) = {
    this.totalInfotons+=totalInfotons.toLong
    neptuneTotalTime+=neptuneDurationMilis
    val neptunetime = DurationFormatUtils.formatDurationWords(neptuneTotalTime, true, true)
    bulkConsumeTotalTime+=readInputStreamDuration
    val bulkConsume = DurationFormatUtils.formatDurationWords(bulkConsumeTotalTime, true, true)
    val totalTime = bulkConsumeTotalTime + neptuneTotalTime
    val overallTime = DurationFormatUtils.formatDurationWords(totalTime, true, true)
    val avgInfotonsPerSec = this.totalInfotons / (totalTime / 1000)
    val summaryLogMsg = "Total Infotons: " + this.totalInfotons.toString.padTo(15, ' ') + "Consume Duration: " + bulkConsume.padTo(25, ' ') +
      "Neptune Ingest Duration: " + neptunetime.padTo(25, ' ') + "total time: " + overallTime.padTo(25, ' ') + "avg Infotons/sec:" + avgInfotonsPerSec
    logger.info(summaryLogMsg)
    print(summaryLogMsg + "\r")
  }

}
