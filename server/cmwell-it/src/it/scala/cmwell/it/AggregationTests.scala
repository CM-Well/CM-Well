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

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Inspectors, Matchers}
import play.api.libs.json.{JsValue, _}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class AggregationTests extends AsyncFunSpec with Matchers with Inspectors with Helpers with LazyLogging {

  describe("Agg API should") {
    val agg = scala.io.Source.fromURL(this.getClass.getResource("/agg/aggnames_293846.nq"))
    val ingestAgg = {
      Http.post(_in, agg.mkString, Some("text/nquads;charset=UTF-8"), List("format" -> "nquads"), tokenHeader)
    }.map { res =>
        withClue(res) {
          res.status should be(200)
          jsonSuccessPruner(Json.parse(res.payload)) shouldEqual jsonSuccess
        }
    }

    agg.close()


    val path = cmw / "test.agg.org" / "Test201903_05_1501_11" / "testStatsApiTerms"

    val aggForIntField = executeAfterCompletion(ingestAgg) {
      spinCheck(100.millis, true)(Http.get(
        uri = path,
        queryParams = List("op" -> "stats", "format" -> "json", "ap" -> "type:term,field::$http://qa.test.rfnt.com/v1.1/testns/num$,size:3")))
      { r =>
        (Json.parse(r.payload) \ "AggregationResponse" \\ "buckets": @unchecked) match {
          case n: collection.IndexedSeq[JsValue] => (r.status == 200) && n.forall(jsonval=> jsonval.as[JsArray].value.size == 3)
        }
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          val total = (Json.parse(res.payload) \ "AggregationResponse" \\ "buckets").map(jsonval=> jsonval.as[JsArray].value.size)
          total should equal (ArrayBuffer(3))
        }
      }
    }


    val aggForExactTextField = executeAfterCompletion(ingestAgg) {
      spinCheck(100.millis, true)(Http.get(
        uri = path,
        queryParams = List("op" -> "stats", "format" -> "json", "ap" -> "type:term,field::$http://qa.test.rfnt.com/v1.1/testns/Test_Data$,size:2")))
      { r =>
        (Json.parse(r.payload) \ "AggregationResponse" \\ "buckets": @unchecked) match {
          case n: collection.IndexedSeq[JsValue] => (r.status == 200) && n.forall(jsonval=> jsonval.as[JsArray].value.size == 2)
        }
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          val total = (Json.parse(res.payload) \ "AggregationResponse" \\ "buckets").map(jsonval=> jsonval.as[JsArray].value.size)
          total should equal (ArrayBuffer(2))
        }
      }
    }


    val badQueryNonExactTextMatch = executeAfterCompletion(ingestAgg) {
      spinCheck(100.millis, true)(Http.get(
        uri = path,
        queryParams = List("op" -> "stats", "format" -> "json", "ap" -> "type:term,field:$http://qa.test.rfnt.com/v1.1/testns/Test_Data$,size:2")))
      { r =>
        Json.parse(r.payload).toString()
          .contains("Stats API does not support non-exact value operator for text fields. Please use :: instead of :") && r.status == 400
      }.map { res =>
        withClue(res) {
          res.status should be(400)
          val result = (Json.parse(res.payload) \ "error").as[String]
          result should include ("Stats API does not support non-exact value operator for text fields. Please use :: instead of :")
        }
      }
    }



    it("ingest aggnames data successfully")(ingestAgg)
    it("get stats for int field")(aggForIntField)
    it("get exact stats for string field")(aggForExactTextField)
    it("get stats for non exact string field should be bad response")(badQueryNonExactTextMatch)

  }

}
