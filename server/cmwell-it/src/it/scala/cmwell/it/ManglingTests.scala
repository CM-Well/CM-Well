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

import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{AsyncFunSpec, Matchers}
import play.api.libs.json.{JsArray, JsDefined, Json}

import scala.concurrent.{Await, ExecutionContext, Future, duration}
import duration.DurationInt

class ManglingTests extends AsyncFunSpec with Matchers with Helpers with LazyLogging {

  describe("test mangling requirements") {

    val firstReq = Http.post(_in,
      """
        |@prefix mang:       <http://s.mangling-ont.com/1.0/pred/> .
        |@prefix owl:        <http://www.w3.org/2002/07/owl#> .
        |
        |<http://example.org/mangling/C0FFEE>
        |      a               owl:Thing ;
        |      mang:someInt    42 ;
        |      mang:someString "Hello, World!"@en-US ;
        |      mang:someDate   "2015-02-02T13:54:0Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        |
        |<http://example.org/mangling/CACA0>
        |      a               owl:Thing ;
        |      mang:someInt    613 ;
        |      mang:someString "רשעים ארורים"@he ;
        |      mang:someDate   "2001-09-11T8:45:0Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
      """.stripMargin, Some("text/n3;charset=UTF-8"), List("format" -> "n3"), tokenHeader).map { res =>
      withClue(res) {
        jsonSuccessPruner(Json.parse(res.payload)) should be(jsonSuccess)
      }
    }

    val waitingResponse = firstReq.flatMap { _ =>

      val startTime = System.currentTimeMillis()

      def wait(): Future[SimpleResponse[String]] = {
        val f = Http.get(cmw / "meta" / "ns" / "EsRgCg" / "someInt", List("format" -> "json"))(SimpleResponse.Implicits.UTF8StringHandler)
        f.flatMap { res =>
          val timeSpent = System.currentTimeMillis() - startTime

          if (res.status != 200 && timeSpent < requestTimeout.toMillis) cmwell.util.concurrent.SimpleScheduler.scheduleFuture(250.millis)(wait())
          else if(res.status != 200) Future.failed(new IllegalStateException(s"got a bad response: $res"))
          else Json.parse(res.payload) \ "fields" \ "mang" match {
            case JsDefined(JsArray(mangs)) if mangs.length == 1 => Future.successful(res)
            case _ => Future.failed(new IllegalStateException(s"got a bad response: $res"))
          }
        }(ExecutionContext.Implicits.global)
      }

      wait()
    }

    val data =
      """
        |@prefix mang:       <http://s.mangling-ont.com/1.0/pred/> .
        |@prefix owl:        <http://www.w3.org/2002/07/owl#> .
        |
        |<http://example.org/mangling/ACDC>
        |      a               owl:Thing ;
        |      mang:someInt    "Ne yavlyayetsya Int ! Ne yavlyayetsya INT !!!"@ru ;
        |      mang:someString "I AM RUSSIAN"@en ;
        |      mang:someDate   "1986-05-11T12:34:56Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
      """.stripMargin

    val secondReq = waitingResponse.flatMap { wRes =>
      Http.post(_in, data, Some("text/n3;charset=UTF-8"), List("format" -> "n3"), tokenHeader).map { res =>
        withClue(wRes -> res) {
          res.status should be(400)
        }
      }
    }

    val thirdReq = secondReq.flatMap { _ =>
      Http.post(_in, data, Some("text/n3;charset=UTF-8"), List("format" -> "n3", "force" -> ""), tokenHeader).map { res =>
        withClue(res) {
          res.status should be(200)
        }
      }
    }

    val fourthReq = thirdReq.flatMap { _ =>
      val multi = """
        |@prefix mang:       <http://s.mangling-ont.com/1.0/pred/> .
        |@prefix owl:        <http://www.w3.org/2002/07/owl#> .
        |
        |<http://example.org/mangling/1337>
        |      a               owl:Thing ;
        |      mang:someInt    666 ;
        |      mang:someString "The Number Of The Beast"@en ;
        |      mang:someDate   "1982-03-22T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
        |      mang:multi      "some string" , 3.14 , 1729 .
      """.stripMargin
      Http.post(_in, multi, Some("text/n3;charset=UTF-8"), List("format" -> "n3"), tokenHeader).map { res =>
        withClue(res) {
          res.status should be(200)
        }
      }
    }

    it("should POST some infoton with a field that have only int values")(firstReq)
    it("should wait until mangling meta data is indexed")(noException should be thrownBy Await.result(waitingResponse,requestTimeout))
    it("should fail to add types when there's a single type with 400 error code")(secondReq)
    it("should succeed to add types when there's a single type by using force parameter")(thirdReq)
    it("should succeed to add a new field with multiple types without force")(fourthReq)
  }
}
