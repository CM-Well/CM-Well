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

import net.liftweb.json.DefaultFormats
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{ContentEncodingHttpClient, DefaultHttpClient}
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

object NeptuneIngester {

  final case class ConnectException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
  protected lazy val logger = LoggerFactory.getLogger("neptune_ingester")

  def ingestToNeptuneViaSparqlAPI(neptuneCluster: String, sparqlCmd: String, ec: ExecutionContext): Future[Int] = Future {
    val client = new ContentEncodingHttpClient
    val httpPost = new HttpPost("http://" + neptuneCluster + "/sparql")
    val entity = new StringEntity(sparqlCmd)
    httpPost.setEntity(entity)
    client.setHttpRequestRetryHandler(new NeptuneIngesterHttpClientRetryHandler())
    httpPost.setHeader("Content-type", "application/x-www-form-urlencoded")
    val response = client.execute(httpPost)
    val statusCode = response.getStatusLine.getStatusCode
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

    def ingestToNeptuneViaLoaderAPI(neptuneCluster: String, fileName:String, ec: ExecutionContext): Future[Int] = Future {
      val client = new ContentEncodingHttpClient
      val httpPost = new HttpPost("http://" + neptuneCluster + "/loader")
      val entity = new StringEntity("{\"source\" : \"https://s3.amazonaws.com/cm-well/sync/" + fileName + "\", \"format\" : \"nquads\", \"iamRoleArn\" :\"arn:aws:iam::113379206287:role/NeptuneLoadFromS3\",  \"mode\": \"NEW\", \"region\" : \"us-east-1\", \"failOnError\" : \"TRUE\"}")
      httpPost.setEntity(entity)
      client.setHttpRequestRetryHandler(new NeptuneIngesterHttpClientRetryHandler())
      httpPost.setHeader("Content-type", "application/json")
      val response = client.execute(httpPost)
      val resBody = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
      val loadId = extractLoadId(resBody)
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode == 200) {
        val loadJobStatus = getLoaderJobStatus(neptuneCluster, loadId)
        client.close()
        loadJobStatus
      } else if (statusCode == 503) {
        throw ConnectException(scala.io.Source.fromInputStream(response.getEntity.getContent).mkString)
      }
      else {
        throw new Throwable(scala.io.Source.fromInputStream(response.getEntity.getContent).mkString)
      }
    }(ec)

    private def getLoaderJobStatus(neptuneCluster: String, loaderId:String):Int = {
      val url = "http://" + neptuneCluster + "/loader/" + loaderId + "?details=true&errors=true&page=1&errorsPerPage=3"
      val client = new DefaultHttpClient
      client.setHttpRequestRetryHandler(new CustomHttpClientRetryHandler())
      val get = new HttpGet(url)
      val response = client.execute(get)
      val statusCode = response.getStatusLine.getStatusCode
      val resBody = scala.io.Source.fromInputStream(response.getEntity.getContent).mkString
      val loadJobstatus = extractLoadJobstatus(resBody)
      if(statusCode == 200 && loadJobstatus == "LOAD_COMPLETED") {
        println("cool, loaded completed")
        client.close()
        statusCode
      }
      else if(statusCode == 200 && loadJobstatus == "LOAD_IN_PROGRESS") {
        Thread.sleep(20000)
        getLoaderJobStatus(neptuneCluster, loaderId)
      }
      else{
        throw new Throwable("Error status code=" + statusCode+ " " + scala.io.Source.fromInputStream(response.getEntity.getContent).mkString)
      }
    }


    private implicit val formats = DefaultFormats

    def extractLoadId(resBody:String):String = {
      val json = resBody.replaceAll("\n", "")
      val parsed = net.liftweb.json.parse(json)
      (parsed \ "payload" \ "loadId").extract[String]
    }

    def extractLoadJobstatus(resBody:String) = {
      val json = resBody.replaceAll("\n", "")
      val parsed = net.liftweb.json.parse(json)
      (parsed \"payload" \ "overallStatus" \ "status").extract[String]
    }




}
