package com.poc

import java.io.IOException
import java.net.UnknownHostException

import org.apache.http.impl.client.DefaultHttpRequestRetryHandler
import org.apache.http.protocol.HttpContext

class CustomHttpClientRetryHandler extends DefaultHttpRequestRetryHandler{

  @Override
   override def retryRequest(exception: IOException, executionCount: Int, context: HttpContext) :Boolean = {
    println("Going to retry http request...")
    if (executionCount >= 3) { // Do not retry if over max retry count
      false
    } else {
      println("Exception=" + exception)
      Thread.sleep(5000)
      true
    }
  }

}
