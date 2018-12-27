package com.poc

import java.io.IOException

import org.apache.http.impl.client.DefaultHttpRequestRetryHandler
import org.apache.http.protocol.HttpContext
import org.slf4j.LoggerFactory

class CustomHttpClientRetryHandler extends DefaultHttpRequestRetryHandler{

  protected lazy val logger = LoggerFactory.getLogger("httP-retry_handler")

  @Override
   override def retryRequest(exception: IOException, executionCount: Int, context: HttpContext) :Boolean = {
    logger.info("Going to retry http request...")
    if (executionCount >= 3) { // Do not retry if over max retry count
      false
    } else {
      logger.error("Exception=" + exception)
      Thread.sleep(5000)
      true
    }
  }

}
