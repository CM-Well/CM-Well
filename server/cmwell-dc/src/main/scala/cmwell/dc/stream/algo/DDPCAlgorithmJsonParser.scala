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
package cmwell.dc.stream.algo

import java.net.URL

import cmwell.dc.LazyLogging
import cmwell.dc.stream.MessagesTypesAndExceptions.AlgoData
import play.api.libs.json.{JsArray, JsDefined, JsLookupResult, JsObject, JsString}

object DDPCAlgorithmJsonParser extends LazyLogging {


  def extractAlgoInfo(f: JsLookupResult):AlgoData = {
  val algoClass = f \ "algoClass" match {
      case JsDefined(JsArray(seq))
        if seq.length == 1 && seq.head.isInstanceOf[JsString] =>
        seq.head.as[String]
    }

    val algoJarUrl = f \ "algoJarUrl" match {
      case JsDefined(JsArray(seq))
        if seq.length == 1 && seq.head.isInstanceOf[JsString] =>
        seq.head.as[String]
    }

    val params = f \ "algoParams" match {
      case JsDefined(JsArray(seq)) =>
        seq.collect {
          case JsString(rule) => rule.split("->") match {
            case Array(source, target) => (source, target)
          }
        }.toMap
      case _ => Map.empty[String, String]
    }
    val url = new URL(algoJarUrl)
    if(url.getHost != "localhost" || !url.getPath.startsWith("/meta"))
      throw new IllegalArgumentException(s"Host is not localhost or url doesn't in /meta, url=$algoJarUrl")
    AlgoData(algoClass, algoJarUrl, params)
  }
}
