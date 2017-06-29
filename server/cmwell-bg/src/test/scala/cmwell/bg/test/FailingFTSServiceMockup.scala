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


package cmwell.bg.test

import cmwell.fts._
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by israel on 06/03/2017.
  */

object FailingFTSServiceMockup {
  def apply(esClasspathYml:String, errorModuloDivisor:Int) = new FailingFTSServiceMockup(ConfigFactory.load(), esClasspathYml, errorModuloDivisor)
}

class FailingFTSServiceMockup(config: Config, esClasspathYaml: String, errorModuloDivisor:Int) extends FTSServiceNew(config, esClasspathYaml) {

  var errorModuloDividend = 0
  var errorCount = 0

  override def executeIndexRequests(indexRequests: Iterable[ESIndexRequest])
                                   (implicit executionContext: ExecutionContext): Future[BulkIndexResult] = {
    errorModuloDividend += 1
    if(errorModuloDividend % errorModuloDivisor == 0)
      Future.successful(new RejectedBulkIndexResult("fake"))
    else
      super.executeIndexRequests(indexRequests)
  }
}
