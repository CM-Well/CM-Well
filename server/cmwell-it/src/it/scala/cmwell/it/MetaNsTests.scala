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


package cmwell.it

import cmwell.it.fixture.NSHashesAndPrefixes
import cmwell.util.formats.JsonEncoder
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Matchers}

import scala.concurrent.duration._

class MetaNsTests extends AsyncFunSpec with Matchers with Helpers with NSHashesAndPrefixes with LazyLogging {
  describe("CM-Well /meta/ns") {

    it("should make sure all meta was uploaded and indexed") {
      val urls = metaNsPaths.mkString("", "\n", "\n")

      spinCheck(100.millis, true)(Http.post(_out, urls, Some("text/plain;charset=UTF-8"), List("format" -> "json"), tokenHeader)){
        case SimpleResponse(_, _, (_, bag)) => {
          JsonEncoder.decodeBagOfInfotons(bag) match {
            case Some(boi) => boi.infotons.length == metaNsPaths.size
            case None => false
          }
        }
      }
      .map {
        case res@SimpleResponse(_, _, (_, bag)) => {
          val infotons = JsonEncoder.decodeBagOfInfotons(bag) match {
            case Some(boi) => boi.infotons
            case None => throw new RuntimeException(s"got bad response: $res")
          }

          withClue {
            val s1 = infotons.map(_.systemFields.path).toSet
            val s2 = metaNsPaths
            s"""
             |s1 (${s1.size}) = infotons.length (${infotons.length})
             |s2 (${s2.size}) = listOfUrls.size (${metaNsPaths.size})
             |s1 &~ s2 = ${s1 &~ s2}
             |s2 &~ s1 = ${s2 &~ s1}
             |""".stripMargin }(infotons.length should be(metaNsPaths.size))
        }
      }
    }
  }
}
