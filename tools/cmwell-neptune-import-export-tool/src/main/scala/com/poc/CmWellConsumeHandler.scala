package com.poc

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

object CmWellConsumeHandler {

  protected lazy val logger = LoggerFactory.getLogger("cm_well_consumer")
  var retryCount = 0
  val maxRetry = 3

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


}
