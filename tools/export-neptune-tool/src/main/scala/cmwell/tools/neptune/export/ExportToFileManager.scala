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

import java.io.{FileWriter, _}
import java.time.Instant
import java.util
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.http.client.methods.CloseableHttpResponse
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class ExportToFileManager(ingestConnectionPoolSize: Int){

  var position: String = _
  val groupSize = 500

  implicit val system = ActorSystem("MySystem")
  implicit val scheduler = system.scheduler
  val executor = Executors.newFixedThreadPool(ingestConnectionPoolSize)
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(executor)
  protected lazy val logger = LoggerFactory.getLogger("export_tool")
  var totalInfotons = 0L
  var bulkConsumeTotalTime = 0L
  var neptuneTotalTime = 0L

  import java.util.concurrent.ArrayBlockingQueue

  val blockingQueue = new ArrayBlockingQueue[Boolean](1)

  def exportToNeptune(sourceCluster: String, neptuneCluster: String, lengthHint: Int, updateMode: Boolean, qp: Option[String], bulkLoader:Boolean,
                      proxyHost:Option[String], proxyPort:Option[Int], s3Directory:String, exportToFile:Boolean, localDirectory:String) = {
    try {
      val toolStartTime = Instant.now()
      if(!updateMode && !PropertiesStore.isAutomaticUpdateModePersist() && !PropertiesStore.isStartTimePersist())
        PropertiesStore.persistStartTime(toolStartTime.toString)
      val persistedPosition = PropertiesStore.retreivePosition()
      val position = persistedPosition.getOrElse(CmWellConsumeHandler.retrivePositionFromCreateConsumer(sourceCluster, lengthHint, qp, updateMode, PropertiesStore.isAutomaticUpdateModePersist(), toolStartTime))
      val actualUpdateMode = PropertiesStore.isAutomaticUpdateModePersist() || updateMode
      consumeBulkAndIngest(position, sourceCluster, neptuneCluster, actualUpdateMode, lengthHint, qp, toolStartTime, bulkLoader,
        proxyHost, proxyPort, s3Directory = s3Directory, retryToolCycle = true, exportToFile = exportToFile, localDirectory = localDirectory)

    } catch {
      case e: Throwable =>
        println("Got a failure during  export after retrying 3 times,error:" + e)
        logger.error("Got a failure during  export after retrying 3 times", e)
        e.printStackTrace()
        executor.shutdown()
        system.terminate()
    }
  }

  def consumeBulkAndIngest(position: String, sourceCluster: String, neptuneCluster: String, updateMode: Boolean, lengthHint: Int, qp: Option[String],
                           toolStartTime:Instant, bulkLoader:Boolean, proxyHost:Option[String], proxyPort:Option[Int], automaticUpdateMode:Boolean = false,
                           retryCount:Int = 5, s3Directory:String, retryToolCycle:Boolean, exportToFile:Boolean, localDirectory:String): CloseableHttpResponse = {
    var startTimeBulkConsumeMillis = System.currentTimeMillis()
    var currentPosition = position
    var res = CmWellConsumeHandler.bulkConsume(sourceCluster, currentPosition, "nquads", updateMode)
    logger.info("Cm-well bulk consume http status=" + res.getStatusLine.getStatusCode)
    while (res.getStatusLine.getStatusCode != 204) {
      var bulkConsumeData:InputStream = null
      var tempfile:File = null
        try {
          bulkConsumeData = res.getEntity.getContent
            readInputStreamAndFilterMeta(bulkConsumeData)
        } catch {
          case e: Throwable if retryCount > 0 =>
            logger.error("Failed to read input stream,", e.getMessage)
            logger.error("Going to retry, retry count=" + retryCount)
            Thread.sleep(10000)
            consumeBulkAndIngest(currentPosition, sourceCluster, neptuneCluster, updateMode, lengthHint, qp, toolStartTime, bulkLoader,
              proxyHost, proxyPort, automaticUpdateMode, retryCount - 1, s3Directory, retryToolCycle, exportToFile, localDirectory)
          case e: Throwable if retryCount == 0 =>
            logger.error("Failed to read input stream from cmwell after retry 5 times..going to shutdown the system")
            sys.exit(0)
        }
      finally {
        deleteTempCmwellFiles(new File(".").getCanonicalPath, ".nq")
        bulkConsumeData.close()
      }
      val endTimeBulkConsumeMilis = System.currentTimeMillis()
      val readInputStreamDuration = endTimeBulkConsumeMilis - startTimeBulkConsumeMillis
      //blocked until neptune ingester thread takes the message from qeuue, which means it's ready for next bulk.
      // this happens only when neptune ingester completed processing the previous bulk successfully and persist the position.
      blockingQueue.put(true)
      logger.info("Going to ingest bulk to neptune...please wait...")
      val nextPosition = res.getAllHeaders.find(_.getName == "X-CM-WELL-POSITION").map(_.getValue).getOrElse("")
      val totalInfotons = res.getAllHeaders.find(_.getName == "X-CM-WELL-N").map(_.getValue).getOrElse("")
      currentPosition = nextPosition
      startTimeBulkConsumeMillis = System.currentTimeMillis()
      res = CmWellConsumeHandler.bulkConsume(sourceCluster, nextPosition, "nquads", updateMode)
      logger.info("Cm-well bulk consume http status=" + res.getStatusLine.getStatusCode)
    }
      //This is an automatic update mode
      val nextPosition = if (!updateMode && !PropertiesStore.isAutomaticUpdateModePersist())
        CmWellConsumeHandler.retrivePositionFromCreateConsumer(sourceCluster, lengthHint, qp, updateMode, true, Instant.parse(PropertiesStore.retrieveStartTime().get))
      else currentPosition
      if (retryToolCycle)
        println("\nExport from cm-well completed successfully, tool wait till new infotons be inserted to cmwell")
      logger.info("Export from cm-well completed successfully, no additional data to consume..trying to re-consume in 0.5 minute")
      Thread.sleep(30000)
      consumeBulkAndIngest(nextPosition, sourceCluster, neptuneCluster, updateMode = true, lengthHint, qp, toolStartTime, bulkLoader, proxyHost,
        proxyPort, automaticUpdateMode = true, s3Directory = s3Directory, retryToolCycle = false, exportToFile = exportToFile, localDirectory = localDirectory)
  }


  private def printBulkStatistics(readInputStreamDuration: Long, totalInfotons: String, neptuneDurationMilis: Long) = {
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

  def readInputStreamAndFilterMeta(inputStream:InputStream):Unit = {
    val targetFile = new File("./cm-well-file-" + System.currentTimeMillis() + ".nq")
    val fw = new FileWriter(targetFile, true)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    reader.lines().filter(line => !line.contains("meta/sys")).iterator.asScala.foreach(line => fw.write(line + "\n"))
  }

  def deleteTempCmwellFiles(directoryName: String, extension: String): Unit = {
    val directory = new File(directoryName)
    FileUtils.listFiles(directory, new WildcardFileFilter(extension), null).asScala.foreach(_.delete())
  }

}



