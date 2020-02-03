/**
  * © 2019 Refinitiv. All Rights Reserved.
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


package cmwell.bg.test

import java.util.concurrent.TimeoutException

import cmwell.fts._
import cmwell.util.concurrent.SimpleScheduler
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by israel on 06/03/2017.
  */

object FailingFTSServiceMockup {
  def apply(config: Config, errorModuloDivisor:Int) = new FailingFTSServiceMockup(config, errorModuloDivisor)
}

class FailingFTSServiceMockup(config: Config, errorModuloDivisor:Int) extends FTSService(config) {

  var errorModuloDividend = 0
  var errorCount = 0

  override def executeIndexRequests(indexRequests: Iterable[ESIndexRequest], forceRefresh:Boolean)
                                   (implicit executionContext: ExecutionContext, logger:Logger = loger): Future[BulkIndexResult] = {
    errorModuloDividend += 1
    if(errorModuloDividend % errorModuloDivisor == 0)
      Future.successful(new RejectedBulkIndexResult("fake"))
    else if(errorModuloDividend % errorModuloDivisor == 2 && errorCount <=2)
      SimpleScheduler.scheduleFuture(15.seconds)(super.executeIndexRequests(indexRequests))
    else
      super.executeIndexRequests(indexRequests)
  }

  /**
    * execute bulk index requests
    *
    * @param indexRequests
    * @param numOfRetries
    * @param waitBetweenRetries
    * @param executionContext
    * @return
    */
  override def executeBulkIndexRequests(indexRequests: Iterable[ESIndexRequest], numOfRetries: Int,
                                        waitBetweenRetries: Long)
                                       (implicit executionContext: ExecutionContext, logger:Logger = loger) = {

    errorModuloDividend += 1
    logger info s"executeBulkIndexRequests: errorModuloDividend=$errorModuloDividend"
    if(errorModuloDividend % errorModuloDivisor == 2 && errorCount <=2 ) {
      errorCount += 1
      logger info s"delaying response"
      throw new TimeoutException("fake")
    }
    else {
        logger info "forwarding to real ftsservice"
      super.executeBulkIndexRequests(indexRequests, numOfRetries, waitBetweenRetries)
    }
  }
}
