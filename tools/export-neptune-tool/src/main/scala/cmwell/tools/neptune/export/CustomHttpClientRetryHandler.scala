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

import java.io.IOException

import org.apache.http.impl.client.DefaultHttpRequestRetryHandler
import org.apache.http.protocol.HttpContext
import org.slf4j.LoggerFactory

class CustomHttpClientRetryHandler extends DefaultHttpRequestRetryHandler{

  protected lazy val logger = LoggerFactory.getLogger("httP-retry_handler")

  @Override
   override def retryRequest(exception: IOException, executionCount: Int, context: HttpContext) :Boolean = {
    logger.info("Going to retry http request...")
    if (executionCount >= 5) { // Do not retry if over max retry count
      false
    } else {
      logger.error("Exception=" + exception)
      Thread.sleep(10000)
      true
    }
  }

}
