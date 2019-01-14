
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

import java.net.{URL, URLEncoder}
import java.time.Instant

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

object CmWellConsumeHandler {

  protected lazy val logger = LoggerFactory.getLogger("cm_well_consumer")
  val maxRetry = 3

  def bulkConsume(cluster: String, position: String, format: String, updateMode:Boolean, retryCount:Int= 0): CloseableHttpResponse = {
    val withMeta = if(updateMode) "&with-meta" else ""
    val url = "http://" + cluster + "/_bulk-consume?position=" + position + "&format=" + format + withMeta
    val client = new DefaultHttpClient
    client.setHttpRequestRetryHandler(new CustomHttpClientRetryHandler())
    val get = new HttpGet(url)
    logger.info("Going to bulk consume,url= " + url)
    val response = client.execute(get)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode != 200 && statusCode != 204) {
      if(statusCode == 503) {
        logger.error("Failed to bulk consume, error status code=" + statusCode + "response entity=" + EntityUtils.toString(response.getEntity) + ".Going to retry...")
        Thread.sleep(2000)
        bulkConsume(cluster, position, format, updateMode)
      }
      else{
        if (retryCount < maxRetry) {
          logger.error("Failed to bulk consume, error status code=" + statusCode + "response entity=" + EntityUtils.toString(response.getEntity) + ".Going to retry...,retry count=" + retryCount)
          Thread.sleep(2000)
          bulkConsume(cluster, position, format, updateMode, retryCount + 1)
        } else {
          throw new Throwable("Failed to consume from cm-well, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
        }
      }
    }
    response
  }

  def retrivePositionFromCreateConsumer(cluster: String, lengthHint: Int, qp: Option[String], updateMode:Boolean, automaticUpdateMode:Boolean, toolStartTime:Instant, retryCount:Int = 0): String = {
    val withDeletedParam = if(updateMode) "&with-deleted" else ""
    //initial mode
    val qpTillToolStartTime = if(!updateMode && !automaticUpdateMode)  URLEncoder.encode(",system.lastModified<<") + toolStartTime.toString else ""
    //automatic update mode
    val qpAfterToolStartTime = if(!updateMode && automaticUpdateMode) URLEncoder.encode(",system.lastModified>>" )+ toolStartTime.toString else ""
    val createConsumerUrl = "http://" + cluster + "/?op=create-consumer&qp=-system.parent.parent_hierarchy:/meta/" + qp.getOrElse("") + qpTillToolStartTime + qpAfterToolStartTime + "&recursive&length-hint=" + lengthHint + withDeletedParam
    val url = createConsumerUrl
    logger.info("create-consumer-url=" + url)
    val get = new HttpGet(url)
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
        retrivePositionFromCreateConsumer(cluster, lengthHint, qp, updateMode, automaticUpdateMode, toolStartTime)
      }else {
        if (retryCount < maxRetry) {
          logger.error("Failed to retrieve position via create-consumer api,error status code=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity) + ".Going to retry..., retry count=" + retryCount)
          Thread.sleep(2000)
          retrivePositionFromCreateConsumer(cluster, lengthHint, qp, updateMode, automaticUpdateMode, toolStartTime, retryCount+1)
        }
        else {
          throw new Throwable("Failed to consume from cm-well, error code status=" + statusCode + ", response entity=" + EntityUtils.toString(response.getEntity))
        }
      }
    }
    res
  }


}
