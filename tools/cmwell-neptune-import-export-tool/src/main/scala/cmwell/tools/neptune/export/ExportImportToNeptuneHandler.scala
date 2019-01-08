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

import java.io.InputStream
import java.net.URLDecoder
import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Scheduler}
import cmwell.tools.neptune.export.NeptuneIngester.ConnectException
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

class ExportImportToNeptuneHandler(ingestConnectionPoolSize: Int) {

  var position: String = _
  val groupSize = 500
  
  implicit val system = ActorSystem("MySystem")
  implicit val scheduler = system.scheduler
  val executor = Executors.newFixedThreadPool(ingestConnectionPoolSize)
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(executor)
  protected lazy val logger = LoggerFactory.getLogger("import_export_tool")

  import java.util.concurrent.ArrayBlockingQueue
  import java.util.concurrent.BlockingQueue

  val blockingQueue = new ArrayBlockingQueue[Boolean](1)

  def exportImport(sourceCluster: String, neptuneCluster: String, lengthHint: Int, updateInfotons: Boolean, qp: Option[String]) = {
    try {
      val position = if (PositionFileHandler.isPositionPersist()) PositionFileHandler.readPosition else CmWellConsumeHandler.retrivePositionFromCreateConsumer(sourceCluster, lengthHint, qp, updateInfotons)
      consumeBulkAndIngest(position, sourceCluster, neptuneCluster, updateInfotons)

    } catch {
      case e: Throwable => logger.error("Got a failure during import-export after retrying 3 times")
        e.printStackTrace()
        executor.shutdown()
        system.terminate()
    }
  }

  def consumeBulkAndIngest(position: String, sourceCluster: String, neptuneCluster: String, updateMode: Boolean, retryCount:Int = 5): CloseableHttpResponse = {
    val startTimeMillis = System.currentTimeMillis()
    val res = CmWellConsumeHandler.bulkConsume(sourceCluster, position, "nquads", updateMode)
    logger.info("Cm-well bulk consume http status=" + res.getStatusLine.getStatusCode)
    if (res.getStatusLine.getStatusCode != 204) {
      val inputStream = res.getEntity.getContent
      var ds: Dataset = DatasetFactory.createGeneral()
      try {
        RDFDataMgr.read(ds, inputStream, Lang.NQUADS)
      }catch{
        case e: Throwable if retryCount > 0 =>
          logger.error("Failed to read input stream,", e.getMessage)
          logger.error("Going to retry, retry count=" + retryCount)
          Thread.sleep(5000)
          consumeBulkAndIngest(position, sourceCluster, neptuneCluster, updateMode, retryCount - 1)
        case e: Throwable if retryCount == 0 =>
          logger.error("Failed to read input stream from cmwell after retry 3 times..going to shutdown the system")
          sys.exit(0)
      }
      val endTimeMillis = System.currentTimeMillis()
      val readInputStreamDuration = (endTimeMillis - startTimeMillis) / 1000
      //blocked until neptune ingester thread takes the message from qeuue, which means it's ready for next bulk.
      // this happens only when neptune ingester completed processing the previous bulk successfully and persist the position.
      blockingQueue.put(true)
      logger.info("Going to ingest bulk to neptune...please wait...")
      buildCommandAndIngestToNeptune(neptuneCluster, ds, position, updateMode, readInputStreamDuration)
      val nextPosition = res.getAllHeaders.find(_.getName == "X-CM-WELL-POSITION").map(_.getValue).getOrElse("")
      val totalBytes = res.getAllHeaders.find(_.getName == "X-CM-WELL-N").map(_.getValue).getOrElse("")
      consumeBulkAndIngest(nextPosition, sourceCluster, neptuneCluster, updateMode)
    }
    else {
      logger.info("Export-Import from cm-well completed successfully, no additional data to consume..trying to re-consume in 0.5 minute")
      Thread.sleep(30000)
      consumeBulkAndIngest(position, sourceCluster, neptuneCluster, updateMode)
    }
    res
  }

  def buildCommandAndIngestToNeptune(neptuneCluster: String, ds:Dataset, position:String, updateMode: Boolean, readInputStreamDuration:Long): Unit = {
    val startTimeMillis = System.currentTimeMillis()
    val graphDataSets = ds.asDatasetGraph()
    val totalBytes = graphDataSets.toString.getBytes("UTF-8").length
    val defaultGraph = graphDataSets.getDefaultGraph
    val defaultGraphTriples = SparqlUtil.getTriplesOfSubGraph(defaultGraph)
    //Default Graph
    val defaultGraphResponse = (if (!defaultGraphTriples.isEmpty) {
      var batchList = defaultGraphTriples.split("\n").toList
      batchList.map(triple => SubjectGraphTriple(SparqlUtil.extractSubjectFromTriple(triple), None, triple))
    } else Nil).toList
    //Named Graph
    import scala.collection.JavaConverters._
    val graphNodes = graphDataSets.listGraphNodes().asScala.toSeq
    val namedGraphsResponse = graphNodes.flatMap { graphNode =>
      var subGraph = graphDataSets.getGraph(graphNode)
      val results = SparqlUtil.getTriplesOfSubGraph(subGraph)
      var batchList = results.split("\n").toList
      batchList.map(triple => SubjectGraphTriple(SparqlUtil.extractSubjectFromTriple(triple), Some(graphNode.toString), triple))
    }
    val allQuads = defaultGraphResponse ++ namedGraphsResponse
    val groupBySubjectList = allQuads.groupBy(subGraphTriple => subGraphTriple.subject)
    val groupedBySizeMap = groupBySubjectList.grouped(groupSize)
    val neptuneFutureResults = groupedBySizeMap.map(subjectToTriples => SparqlUtil.buildGroupedSparqlCmd(subjectToTriples.keys, subjectToTriples.values, updateMode))
      .map(sparqlCmd => postWithRetry(neptuneCluster, sparqlCmd))

    Future.sequence(neptuneFutureResults.toList).onComplete(_ => {
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      logger.info("Bulk Statistics: Duration of ingest to neptune:" + durationSeconds + " seconds, total bytes :" + totalBytes + "===total time===" + (readInputStreamDuration + durationSeconds))
      logger.info("About to persist position=" + position)
      PositionFileHandler.persistPosition(position)
      logger.info("Persist position successfully")
      blockingQueue.take()
    })

  }


  def postWithRetry(neptuneCluster: String, sparqlCmd: String): Future[Int] = {
    retry(10.seconds, 3) {
      NeptuneIngester.ingestToNeptune(neptuneCluster, sparqlCmd, ec)
    }
  }


  def retry[T](delay: FiniteDuration, retries: Int = -1)(task: => Future[Int])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[Int] = {
    task.recoverWith {
      case e: ConnectException =>
        logger.error("Failed to ingest,", e.getMessage)
        logger.error("Going to retry till neptune will be available")
        akka.pattern.after(delay, scheduler)(retry(delay)(task))
      case e: Throwable if retries > 0 =>
        logger.error("Failed to ingest,", e.getMessage)
        logger.error("Going to retry, retry count=" + retries)
        akka.pattern.after(delay, scheduler)(retry(delay, retries - 1)(task))
      case e: Throwable if retries == 0 =>
        logger.error("Failed to ingest to neptune after retry 3 times..going to shutdown the system")
        sys.exit(0)
    }
  }

}




