package com.poc

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.URLEncoder
import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Scheduler}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{ContentEncodingHttpClient, DefaultHttpClient}
import org.apache.http.util.EntityUtils
import org.apache.jena.graph.{Graph, Node}
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

class ExportImportToNeptuneHandler(ingestConnectionPoolSize: Int) {

  var position: String = _
  var retryCount = 0
  val maxRetry = 3
  val groupSize = 100
  implicit val system = ActorSystem("MySystem")
  implicit val scheduler = system.scheduler
  val executor = Executors.newFixedThreadPool(ingestConnectionPoolSize)
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(executor)
  protected lazy val logger = LoggerFactory.getLogger("import_export_tool")

  final case class ConnectException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
  case class SubjectGraphTriple(subject: String, graph: Option[String], triple:String)


  def exportImport(sourceCluster: String, neptuneCluster: String, lengthHint: Int, updateInfotons: Boolean, qp: Option[String], withDeleted:Boolean) = {
    try {
      val position = if (PositionFileHandler.isPositionPersist()) PositionFileHandler.readPosition else retrivePositionFromCreateConsumer(sourceCluster, lengthHint, qp, withDeleted)
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
    val res = bulkConsume(sourceCluster, position, "nquads", withDeleted)
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds1 = (endTimeMillis - startTimeMillis) / 1000
    println("bulk consume time:" + durationSeconds1)
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
    val defaultGraphTriples = getTriplesOfSubGraph(defaultGraph)
    //Default Graph
    val defaultGraphResponse = (if (!defaultGraphTriples.isEmpty) {
      var batchList = defaultGraphTriples.split("\n").toList
      val seq = batchList.map(triple => SubjectGraphTriple(extractSubjectFromTriple(triple), None, triple))
      seq
    } else Nil).toList
    //Named Graph
    import scala.collection.JavaConverters._
    val graphNodes = graphDataSets.listGraphNodes().asScala.toSeq
    val namedGraphsResponse = graphNodes.flatMap { graphNode =>
      var subGraph = graphDataSets.getGraph(graphNode)
      val results = getTriplesOfSubGraph(subGraph)
      var batchList = results.split("\n").toList
      val seq2 = batchList.map(triple => SubjectGraphTriple(extractSubjectFromTriple(triple), Some(graphNode.toString), triple))
      seq2
    }
    val allQuads = defaultGraphResponse ++ namedGraphsResponse
    val groupBySubjectList = allQuads.groupBy(subGraphTriple => subGraphTriple.subject)
    val groupedBySizeMap = groupBySubjectList.grouped(groupSize)
    val neptuneFutureResults = groupedBySizeMap.map(x => buildGroupedSparqlCmd(x.keys, x.values, updateMode, withDeleted))
      .map(sparqlCmd => postWithRetry(neptuneCluster, sparqlCmd))
    neptuneFutureResults.toList

  }

  private def extractSubjectFromTriple(triple: String) = {
    triple.split(" ")(0)
  }

  private def getTriplesOfSubGraph(subGraph:Graph):String  = {
     val tempOs = new ByteArrayOutputStream
     RDFDataMgr.write(tempOs, subGraph, Lang.NTRIPLES)
     new String(tempOs.toByteArray, "UTF-8")
   }


  //Default Graph
  def buildGroupedSparqlCmd(subjects: Iterable[String], allSubjGraphTriples: Iterable[List[SubjectGraphTriple]], updateMode: Boolean, withDeleted:Boolean): String = {
    var sparqlCmd = "update="
    val deleteSubj = if (updateMode) Some(subjects.map(subject => "delete where { " + encode(subject) + " ?anyPred ?anyObj};").mkString) else None
    val insertcmd = allSubjGraphTriples.flatten.filterNot(trio => predicateContainsMeta(trio, withDeleted)).map(trio => "INSERT DATA" + trio.graph.fold("{" + encode(trio.triple) + "};")(graph => "{" + "GRAPH <" + encode(graph) + ">{" + encode(trio.triple) + "}};"))
    sparqlCmd + deleteSubj.getOrElse("") + insertcmd.mkString
  }

  private def encode(str: String) = {
    URLEncoder.encode(str, "UTF-8")
  }

   private def predicateContainsMeta(trio: SubjectGraphTriple, withDeleted:Boolean): Boolean = {
     if(!withDeleted) false
     else trio.triple.split(" ")(1).contains("meta/sys")
   }


  def bulkConsume(cluster: String, position: String, format: String, withDeleted:Boolean): CloseableHttpResponse = {
     val withMeta = if(withDeleted) "&with-meta" else ""
    val url = "http://" + cluster + "/_bulk-consume?position=" + position + "&format=" + format + withMeta
    val get = new HttpGet(url)
    logger.info("Going to bulk consume,url= " + url)
    val client = new DefaultHttpClient
    client.setHttpRequestRetryHandler(new CustomHttpClientRetryHandler())
    val response = client.execute(get)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode != 200 && statusCode != 204) {
       if(statusCode == 503) {
         logger.error("Failed to bulk consume, error status code=" + statusCode + "response entity=" + EntityUtils.toString(response.getEntity) + ".Going to retry...")
         Thread.sleep(2000)
         bulkConsume(cluster, position, format, withDeleted)
       }
      else{
         if (retryCount < maxRetry) {
           logger.error("Failed to bulk consume, error status code=" + statusCode + "response entity=" + EntityUtils.toString(response.getEntity) + ".Going to retry...,retry count=" + retryCount)
           Thread.sleep(2000)
           retryCount += 1
           bulkConsume(cluster, position, format, withDeleted)
         } else {
           retryCount = 0
           throw new Throwable("Failed to consume from cm-well, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
         }
       }
    }
    response
  }


  def retrivePositionFromCreateConsumer(cluster: String, lengthHint: Int, qp: Option[String], withDeleted:Boolean): String = {
    val withDeletedParam = if(withDeleted) "&with-deleted" else ""
    val createConsumerUrl = "http://" + cluster + "/?op=create-consumer&qp=-system.parent.parent_hierarchy:/meta/" + qp.getOrElse("") + "&recursive&length-hint=" + lengthHint + withDeletedParam
    logger.info("create-consumer-url=" + createConsumerUrl)
    val get = new HttpGet(createConsumerUrl)
    val client = new DefaultHttpClient
    client.setHttpRequestRetryHandler(new CustomHttpClientRetryHandler())
    val response = client.execute(get)
    val res = response.getAllHeaders.find(_.getName == "X-CM-WELL-POSITION").map(_.getValue).getOrElse("")
    logger.info("create-Consumer http status=" + response.getStatusLine.getStatusCode)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode != 200) {
      if(statusCode == 503){
         logger.error("Failed to retrieve position via create-consumer api,error status code=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity) + ".Going to retry...")
        Thread.sleep(2000)
        retrivePositionFromCreateConsumer(cluster, lengthHint, qp, withDeleted)
      }else {
        if (retryCount < maxRetry) {
           logger.error("Failed to retrieve position via create-consumer api,error status code=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity) + ".Going to retry..., retry count=" + retryCount)
          Thread.sleep(2000)
          retryCount += 1
          retrivePositionFromCreateConsumer(cluster, lengthHint, qp, withDeleted)
        }
        else {
          retryCount = 0
          throw new Throwable("Failed to consume from cm-well, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
        }
      }
    }
    res
  }

  def ingestToNeptune(neptuneCluster: String, sparqlCmd: String): Future[Int] = Future {
    val client = new ContentEncodingHttpClient
    val httpPost = new HttpPost("http://" + neptuneCluster + "/sparql")
    val entity = new StringEntity(sparqlCmd)
    httpPost.setEntity(entity)
    httpPost.setHeader("Content-type", "application/x-www-form-urlencoded")
    val response = client.execute(httpPost)
    val statusCode = response.getStatusLine.getStatusCode
    println("statuscode=" + statusCode)
    if (statusCode == 200) {
      client.close()
      statusCode
    } else if (statusCode == 503) {
      throw ConnectException("Failed to ingest to Neptune, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
    }
    else {
      throw new Throwable("Failed to ingest to Neptune, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
    }
  }(ec)


  def postWithRetry(neptuneCluster: String, sparqlCmd: String): Future[Int] = {
    retry(10.seconds, 3) {
      ingestToNeptune(neptuneCluster, sparqlCmd)
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


object ExportImportMain extends App {
  val exportImportHandler = new ExportImportToNeptuneHandler(5)
  exportImportHandler.exportImport("localhost:9000", "localhost:9999", 16000, true, None, true)

//    import java.io.InputStream
//    val source = "<http://lish1> <meta/sys> <o1> .\n<http://s1> <e1> <o1> <g1> .\n<http://lish2> <p2> <o2> <g2> .\n<http://lish1> <p11> <o11> .\n<http://s4> <p4> <o4> <g4> .\n"
//    val in: InputStream = org.apache.commons.io.IOUtils.toInputStream(source, "UTF-8")
//    val res = exportImportHandler.buildCommandAndIngestToNeptune("localhost:9999", in, true, true)

}





