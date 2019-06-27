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
package cmwell.dc.stream.fingerprint

import cmwell.dc.stream.MessagesTypesAndExceptions.FingerPrintData
import play.api.libs.json.{JsArray, JsDefined, JsLookupResult, JsObject}

object FingerPrintJsonParser {


  def extractFingerPrintInfo(f: JsLookupResult):Option[FingerPrintData] = {
    f \ "dcInfoExtra" match {
      case JsDefined(JsArray(seq)) =>
        seq.headOption.collect {
          case JsObject(extraInfo) => FingerPrintData(extraInfo("webServiceCluster").as[String], extraInfo("destinationCluster").as[String])
          case _ => throw new IllegalArgumentException("missing dcInfoExtra for finger print")
        }
      case _ => None
    }
  }

}
