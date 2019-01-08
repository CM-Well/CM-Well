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

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.ContentEncodingHttpClient
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

object NeptuneIngester {

  final case class ConnectException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
  protected lazy val logger = LoggerFactory.getLogger("neptune_ingester")

  def ingestToNeptune(neptuneCluster: String, sparqlCmd: String, ec: ExecutionContext): Future[Int] = Future {
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


}
