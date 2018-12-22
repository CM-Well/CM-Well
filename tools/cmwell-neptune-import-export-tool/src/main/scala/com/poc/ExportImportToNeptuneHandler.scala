package com.poc

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.URLEncoder
import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Scheduler}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{ContentEncodingHttpClient, DefaultHttpClient}
import org.apache.http.util.EntityUtils
import org.apache.jena.graph.Node
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}


class ExportImportToNeptuneHandler(ingestThreadsNum:Int){

  var position :String = _
  var retryCount = 0
  val maxRetry = 3
  val groupSize = 200
  implicit val system =  ActorSystem("MySystem")
  implicit val scheduler = system.scheduler
  val executor = Executors.newFixedThreadPool(ingestThreadsNum)
  implicit val ec:ExecutionContext = scala.concurrent.ExecutionContext.fromExecutor(executor)


  def exportImport(sourceCluster: String, neptuneCluster: String) = {
    try {
      val position = if (PositionFileHandler.isPositionPersist()) PositionFileHandler.readPosition else retrivePositionFromCreateConsumer(sourceCluster)
      consumeBulkAndIngest(position, sourceCluster, neptuneCluster)

    } catch {
      case e: Throwable => println("Got a failure during import-export after retrying 3 times")
        e.printStackTrace()
        executor.shutdown()
        system.terminate()
    }
  }

  def consumeBulkAndIngest(position:String, sourceCluster: String, neptuneCluster: String):CloseableHttpResponse = {
    val startTimeMillis = System.currentTimeMillis()
    var res = bulkConsume(sourceCluster, position, "nquads")
    println("Cm-well bulk consume http status=" + res.getStatusLine.getStatusCode)
    if (res.getStatusLine.getStatusCode != 204) {
      println("Bulk consume http status=" + res.getStatusLine.getStatusCode)
      val inputStream = res.getEntity.getContent
      print("Going to ingest bulk to neptune...")
      val ingestFuturesList = buildCommandAndIngestToNeptune(neptuneCluster, inputStream)
      val nextPosition = res.getAllHeaders.find(_.getName == "X-CM-WELL-POSITION").map(_.getValue).getOrElse("")
      Future.sequence(ingestFuturesList).onComplete(_ => {
        val endTimeMillis = System.currentTimeMillis()
        val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
        println("Bulk Statistics: Duration for bulk consume and ingest to neptune:" + durationSeconds + "seconds")
        println("About to persist position=" + nextPosition)
        PositionFileHandler.persistPosition(nextPosition)
        println("Persist position successfully")
        consumeBulkAndIngest(nextPosition, sourceCluster, neptuneCluster)
      })
    } else{
      println("Export-Import from cm-well completed successfully")
      executor.shutdown()
      system.terminate()
    }
    res
  }


  def buildCommandAndIngestToNeptune(neptuneCluster: String, batch: InputStream): List[Future[Int]]= {
    var ds: Dataset = DatasetFactory.createGeneral()
    RDFDataMgr.read(ds, batch, Lang.NQUADS)
    val graphDataSets = ds.asDatasetGraph()
    val defaultGraph = graphDataSets.getDefaultGraph
    val tempOs = new ByteArrayOutputStream
    RDFDataMgr.write(tempOs, defaultGraph, Lang.NTRIPLES)
    val defaultGraphTriples = new String(tempOs.toByteArray, "UTF-8")
    //Default Graph
    val defaultGraphResponse = (if (!defaultGraphTriples.isEmpty) {
      var batchList = defaultGraphTriples.split("\n").toList
      val seq = batchList.grouped(groupSize).map(triples => URLEncoder.encode(triples.mkString, "UTF-8"))
        .map(triplesStr => buildSparqlCmd(triplesStr))
        .map(sparqlCmd => postWithRetry(neptuneCluster, sparqlCmd))
      seq
    } else Nil).toList
    //Named Graph
    import scala.collection.JavaConverters._
    val graphNodes = graphDataSets.listGraphNodes().asScala.toSeq
    val namedGraphsResponse = graphNodes.flatMap { graphNode =>
      val tempOs = new ByteArrayOutputStream
      var subGraph = graphDataSets.getGraph(graphNode)
      RDFDataMgr.write(tempOs, subGraph, Lang.NTRIPLES)
      val results = new String(tempOs.toByteArray, "UTF-8")
      var batchList = results.split("\n").toList
      val seq2 = batchList.grouped(groupSize).map(triples => URLEncoder.encode(triples.mkString, "UTF-8"))
        .map(triplesStr => buildSparqlCmd(triplesStr, Some(graphNode)))
        .map(sparqlCmd => postWithRetry(neptuneCluster, sparqlCmd))
      seq2
    }

    val allFutures = defaultGraphResponse ++ namedGraphsResponse
    allFutures

  }

  def buildSparqlCmd(groupedTriples: String, graphName: Option[Node] = None): String = {
    if(graphName.isEmpty){
      "update=INSERT DATA" +
        "{" +
        groupedTriples +
        "}"
    }else {
      "update=INSERT DATA" +
        "{" +
        "GRAPH <" + graphName.get + ">{" +
        groupedTriples +
        "}}"
    }
  }

  def bulkConsume(cluster: String, position: String, format: String): CloseableHttpResponse = {
    println("About to consume from cm-well")
    val url = "http://" + cluster + "/_bulk-consume?position=" + position + "&format=" + format
    val get = new HttpGet(url)
    println("Going to bulk consume,url= " + url)
    val client = new DefaultHttpClient
    client.setHttpRequestRetryHandler(new CustomHttpClientRetryHandler())
    val response = client.execute(get)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode != 200 && statusCode != 204) {
      if (retryCount < maxRetry) {
        println("Failed to bulk consume, going to retry, retry count =" + retryCount)
        Thread.sleep(2000)
        retryCount += 1
        bulkConsume(cluster, position, format)
      } else {
        retryCount = 0
        throw new Throwable("Failed to consume from cm-well, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
      }
    }
    response
  }


  def retrivePositionFromCreateConsumer(cluster: String): String = {
    val get = new HttpGet("http://" + cluster + "/?op=create-consumer&qp=-system.parent.parent_hierarchy:/meta/&recursive&length-hint=16000")
    val client = new DefaultHttpClient
    client.setHttpRequestRetryHandler(new CustomHttpClientRetryHandler())
    val response = client.execute(get)
    val res = response.getAllHeaders.find(_.getName == "X-CM-WELL-POSITION").map(_.getValue).getOrElse("")
    println("create-Consumer http status=" + response.getStatusLine.getStatusCode)
    println("create-consumer token =" + res)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode != 200) {
      if (retryCount < maxRetry) {
        println("Failed to retrieve position via create-consumer api, going to retry, retry count =" + retryCount)
        Thread.sleep(2000)
        retryCount += 1
        retrivePositionFromCreateConsumer(cluster)
      } else {
        retryCount = 0
        throw new Throwable("Failed to consume from cm-well, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
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
    if (statusCode == 200) {
      client.close()
      statusCode
    } else {
      throw new Throwable("Failed to ingest to Neptune, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
    }
  }(ec)

  def postWithRetry(neptuneCluster:String, sparqlCmd: String): Future[Int] = {
    retry(10.seconds, 3){ingestToNeptune(neptuneCluster, sparqlCmd)}
  }


  def retry[T](delay: FiniteDuration, retries: Int)(task: => Future[Int])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[Int] = {
    task.recoverWith {
      case e: Throwable if retries > 0 =>
        println("Failed to ingest,", e.getMessage)
        println("Going to retry, retry count=" + retries)
        akka.pattern.after(delay, scheduler)(retry(delay, retries - 1)(task))
      case e: Throwable if retries == 0 =>
        println("Failed to ingest to neptune after retry 3 times..going to shutdown the system")
        sys.exit(0)
    }
  }
}





