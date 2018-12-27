package com.poc

import java.io.InputStream
import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Scheduler}
import com.poc.NeptuneIngester.ConnectException
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

class ExportImportToNeptuneHandler(ingestConnectionPoolSize: Int) {

  var position: String = _
  val groupSize = 100
  
  implicit val system = ActorSystem("MySystem")
  implicit val scheduler = system.scheduler
  val executor = Executors.newFixedThreadPool(ingestConnectionPoolSize)
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(executor)
  protected lazy val logger = LoggerFactory.getLogger("import_export_tool")


  def exportImport(sourceCluster: String, neptuneCluster: String, lengthHint: Int, updateInfotons: Boolean, qp: Option[String], withDeleted:Boolean) = {
    try {
      val position = if (PositionFileHandler.isPositionPersist()) PositionFileHandler.readPosition else CmWellConsumeHandler.retrivePositionFromCreateConsumer(sourceCluster, lengthHint, qp, withDeleted)
      consumeBulkAndIngest(position, sourceCluster, neptuneCluster, updateInfotons, withDeleted)

    } catch {
      case e: Throwable => logger.error("Got a failure during import-export after retrying 3 times")
        e.printStackTrace()
        executor.shutdown()
        system.terminate()
    }
  }

  def consumeBulkAndIngest(position: String, sourceCluster: String, neptuneCluster: String, updateMode: Boolean, withDeleted:Boolean): CloseableHttpResponse = {
    val startTimeMillis = System.currentTimeMillis()
    val res = CmWellConsumeHandler.bulkConsume(sourceCluster, position, "nquads", withDeleted)
    logger.info("Cm-well bulk consume http status=" + res.getStatusLine.getStatusCode)
    if (res.getStatusLine.getStatusCode != 204) {
      val inputStream = res.getEntity.getContent
      logger.info("Going to ingest bulk to neptune...please wait...")
      val startTimeMillis = System.currentTimeMillis()
      val ingestFuturesList = buildCommandAndIngestToNeptune(neptuneCluster, inputStream, updateMode, withDeleted)
      val nextPosition = res.getAllHeaders.find(_.getName == "X-CM-WELL-POSITION").map(_.getValue).getOrElse("")
      val totalBytes = res.getAllHeaders.find(_.getName == "X-CM-WELL-N").map(_.getValue).getOrElse("")
      Future.sequence(ingestFuturesList).onComplete(_ => {
        val endTimeMillis = System.currentTimeMillis()
        val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
        logger.info("Bulk Statistics: Duration for bulk consume and ingest to neptune:" + durationSeconds + " seconds, total infotons :" + totalBytes)
        logger.info("About to persist position=" + nextPosition)
        PositionFileHandler.persistPosition(nextPosition)
        logger.info("Persist position successfully")
        consumeBulkAndIngest(nextPosition, sourceCluster, neptuneCluster, updateMode, withDeleted)
      })
    }
    else {
      logger.info("Export-Import from cm-well completed successfully, no additional data to consume..trying to re-consume in 0.5 minute")
      Thread.sleep(30000)
      consumeBulkAndIngest(position, sourceCluster, neptuneCluster, updateMode, withDeleted)
      //      executor.shutdown()
      //      system.terminate()
    }
    res
  }

  def buildCommandAndIngestToNeptune(neptuneCluster: String, inputStream: InputStream, updateMode: Boolean, withDeleted:Boolean): List[Future[Int]] = {
    var ds: Dataset = DatasetFactory.createGeneral()
    RDFDataMgr.read(ds, inputStream, Lang.NQUADS)
    val graphDataSets = ds.asDatasetGraph()
    val defaultGraph = graphDataSets.getDefaultGraph
    val defaultGraphTriples = SparqlUtil.getTriplesOfSubGraph(defaultGraph)
    //Default Graph
    val defaultGraphResponse = (if (!defaultGraphTriples.isEmpty) {
      var batchList = defaultGraphTriples.split("\n").toList
      val seq = batchList.map(triple => SubjectGraphTriple(SparqlUtil.extractSubjectFromTriple(triple), None, triple))
      seq
    } else Nil).toList
    //Named Graph
    import scala.collection.JavaConverters._
    val graphNodes = graphDataSets.listGraphNodes().asScala.toSeq
    val namedGraphsResponse = graphNodes.flatMap { graphNode =>
      var subGraph = graphDataSets.getGraph(graphNode)
      val results = SparqlUtil.getTriplesOfSubGraph(subGraph)
      var batchList = results.split("\n").toList
      val seq2 = batchList.map(triple => SubjectGraphTriple(SparqlUtil.extractSubjectFromTriple(triple), Some(graphNode.toString), triple))
      seq2
    }
    val allQuads = defaultGraphResponse ++ namedGraphsResponse
    val groupBySubjectList = allQuads.groupBy(subGraphTriple => subGraphTriple.subject)
    val groupedBySizeMap = groupBySubjectList.grouped(groupSize)
    val neptuneFutureResults = groupedBySizeMap.map(subjectToTriples => SparqlUtil.buildGroupedSparqlCmd(subjectToTriples.keys, subjectToTriples.values, updateMode, withDeleted))
      .map(sparqlCmd => postWithRetry(neptuneCluster, sparqlCmd))
    neptuneFutureResults.toList

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





