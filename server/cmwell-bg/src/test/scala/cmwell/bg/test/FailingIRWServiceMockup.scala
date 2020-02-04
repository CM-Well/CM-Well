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

import cmwell.domain.Infoton
import cmwell.driver.Dao
import cmwell.irw.{ConsistencyLevel, IRWServiceNativeImpl2}
import cmwell.util.Box
import com.datastax.driver.core.exceptions.DriverException

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by israel on 19/06/2016.
  */
case class FailingIRWServiceMockup(storageDao:Dao, errorModuloDivisor:Int) extends IRWServiceNativeImpl2(storageDao, 25, true) {

  var errorModuloDividend = 0
  var errorCount = 0

  override def readUUIDAsync(uuid: String, level: ConsistencyLevel, dontFetchPayload: Boolean = false)
                            (implicit ec: ExecutionContext): Future[Box[Infoton]] = {
    errorModuloDividend += 1
      logger debug s"readUUIDAsync for uuid:$uuid. errorModuloDividend=$errorModuloDividend"
    if(errorCount < 3 && errorModuloDividend % errorModuloDivisor == 0) {
      errorCount += 1
      logger debug s"throwing DriverException in readUUIDAsync for uuid:$uuid"
      throw new DriverException("test exception")
    } else
      super.readUUIDAsync(uuid, level)
  }


  override def readPathAsync(path: String, level: ConsistencyLevel)
                            (implicit ec: ExecutionContext): Future[Box[Infoton]] = {

    errorModuloDividend += 1
    logger debug s"readPathAsync for path:$path. errorModuloDividend=$errorModuloDividend"
    if(errorCount < 3 && errorModuloDividend % errorModuloDivisor == 0){
      errorCount += 1
      logger debug s"throwing DriverException in readPathAsync for infoton path:${path}"
      throw new DriverException("test exception")
    } else {
      super.readPathAsync(path, level)
    }
  }



  override def writeAsync(infoton: Infoton, level: ConsistencyLevel, skipSetPathLast: Boolean)
                         (implicit ec: ExecutionContext): Future[Infoton] = {

    errorModuloDividend += 1
    logger debug s"writeAsync for path:${infoton.systemFields.path}. errorModuloDividend=$errorModuloDividend"
    if(errorCount < 3 && errorModuloDividend % errorModuloDivisor == 0){
      logger debug s"errorModuloDividend:$errorModuloDividend % errorModuloDivisor:$errorModuloDivisor = ${errorModuloDividend % errorModuloDivisor}"
      errorCount += 1
      logger debug s"throwing DriverException in writeAsync for infoton path:${infoton.systemFields.path}, uuid:${infoton.uuid}"
      throw new DriverException("test exception")
    } else
      super.writeAsync(infoton, level, skipSetPathLast)
  }
}
