package cmwell.analytics.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.client.{ClientProtocolException, ResponseHandler}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * Simple utilities for HTTP requests.
  */
object HttpUtil {

  private val mapper = new ObjectMapper()

  private val responseHandler = new ResponseHandler[String]() {

    override def handleResponse(response: HttpResponse): String = {
      val status = response.getStatusLine.getStatusCode
      if (status >= 200 && status < 300) {
        val entity = response.getEntity

        if (entity != null)
          EntityUtils.toString(entity)
        else
          throw new ClientProtocolException("Unexpected null response")
      }
      else {
        throw new ClientProtocolException("Unexpected response status: " + status)
      }
    }
  }

  def get(url: String, client: CloseableHttpClient): String = {
    val httpGet = new HttpGet(url)
    client.execute(httpGet, responseHandler)
  }

  def get(url: String): String = {

    val client =  HttpClients.createDefault
    try {
      get(url, client)
    }
    finally {
      client.close()
    }
  }

  def getJson(url: String): JsonNode = {
    val client =  HttpClients.createDefault
    try {
      getJson(url, client)
    }
    finally {
      client.close()
    }
  }

  def getJson(url: String, client: CloseableHttpClient): JsonNode = {
    mapper.readTree(get(url, client))
  }

  def getJson(url: String, content: String, contentType: String): JsonNode = {
    val httpPost = new HttpPost(url)
    val entity = new StringEntity(content, ContentType.create(contentType, "UTF-8"))
    httpPost.setEntity(entity)

    val client = HttpClients.createDefault
    try {
      mapper.readTree(client.execute(httpPost, responseHandler))
    }
    finally {
      client.close()
    }
  }
}
