package com.poc

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.ContentEncodingHttpClient
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

object NeptuneIngester {

  final case class ConnectException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
  protected lazy val logger = LoggerFactory.getLogger("neptune_ingester")

  def ingestToNeptune(neptuneCluster: String, sparqlCmd: String,  ec: ExecutionContext): Future[Int] = Future {
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


}
