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


import play.api.libs.json.{JsArray, JsValue}

import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Try}

/**
 * Created by yaakov on 3/2/15.
 */
trait Helpers {
  implicit class JsValueExtensions(v: JsValue) {
    def getValue =  withPrettyException({(v\"value").as[String]}, "getValue")
    def getQuad = withPrettyException({(v\"quad").as[String]}, "getQuad")
    def getArr(prop: String): collection.Seq[JsValue] =
      withPrettyException(_=>{((v \ prop).get: @unchecked) match { case JsArray(seq) => seq }},"getArr",prop)

    def getFirst(prop: String) = getArr(prop).head
    def getValueOfFirst(prop: String) = getFirst(prop).getValue

    private def withPrettyException(f: => String, desc: String):String = Try(f).getOrElse(throw new Exception(s"Operation $desc failed on $v"))
    private def withPrettyException(f: String => collection.Seq[JsValue], desc: String, prop: String):collection.Seq[JsValue] = {
      Try(f(prop)).recoverWith {
        case t: Throwable =>
          Failure(new Exception(s"""Operation $desc("$prop") failed on $v""",t))
      }.get
    }
  }
}

