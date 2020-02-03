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

import cmwell.util.concurrent.FutureTimeout
import cmwell.util.concurrent.SimpleScheduler.scheduleFuture
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.enablers.Size
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.{AsyncFunSpec, Inspectors, Matchers}

class OWTests extends AsyncFunSpec with Matchers with Inspectors with Helpers with fixture.NSHashesAndPrefixes with LazyLogging {

  //Assertions
  val firstIngest = {
    val h1 = scala.io.Source.fromURL(this.getClass.getResource(s"/feed/history.feed.tms.1.nq")).mkString
    Http.post(_ow, h1, Some("text/nquads;charset=UTF-8"), List("format" -> "nquads"), tokenHeader).map { res =>
      withClue(res){
        res.status should be(200)
        jsonSuccessPruner(Json.parse(res.payload)) should be(jsonSuccess)
      }
    }
  }

  def futureToWaitFor(httpReq: =>Future[SimpleResponse[Array[Byte]]], expectedTotal: Int) = {
    val startTime = System.currentTimeMillis()
    def recurse(): Future[SimpleResponse[Array[Byte]]] = {
      httpReq.flatMap { res =>
        Json.parse(res.payload) \ "results" \ "total" match {             //make sure parents are created as well.
          case JsDefined(JsNumber(n)) if n.intValue == expectedTotal => Future.successful(res)
          case _ if System.currentTimeMillis() - startTime > 60000L => Future.failed(new IllegalStateException(s"timed out with last response: $res"))
          case _ => scheduleFuture(990.millis)(recurse())
        }
      }
    }
    recurse()
  }

  val parent = cmw / "feed.tms"
  val path = parent / "1.0"
  lazy val waitForIngest = futureToWaitFor(Http.get(path, List("op" -> "search", "recursive" -> "", "length" -> "1", "format" -> "json")),83)
  lazy val waitForParent = waitForIngest.flatMap(_ =>  futureToWaitFor(Http.get(parent, List("op" -> "search", "length" -> "1", "format" -> "json")),1))
  def executeAfterIngest[T](body: =>Future[T]): Future[T] = waitForIngest.flatMap(_ => body)

  implicit val jsValueArrSize = new Size[JsArray] {
    def sizeOf(obj: JsArray): Long = obj.value.size
  }

  val childrenIndexed = executeAfterIngest {
    Http.get(path, List("op" -> "search", "recursive" -> "", "length" -> "1", "format" -> "json")).map { res =>
      withClue(res) {
        res.status should be(200)
        Json.parse(res.payload) \ "results" \ "total" match {             //make sure parents are created as well.
          case JsDefined(JsNumber(n)) => n should be(83)
          case JsDefined(j) => fail(s"got something else: ${Json.stringify(j)}")
          case u:JsUndefined => fail(s"failed to parse json, error: ${u.error}, original json: ${new String(res.payload, "UTF-8")}")
        }
      }
    }
  }

  val makeSureParentDirsWereNotCreated = executeAfterIngest {
    Http.get(path,List("length" -> "100","format"->"json")).map { res =>
      withClue(res) {
        res.status should be(404)
      }
    }
  }

  val makeSureGrandParentDirsWereNotCreated = makeSureParentDirsWereNotCreated.flatMap { _ =>
    Http.get(cmw / "feed.tms").map { res =>
      withClue(res) {
        res.status should be(404)
      }
    }
  }

  val ingestFeedTMS = makeSureGrandParentDirsWereNotCreated.flatMap{ _ =>
    val payload =
      """
        |<http://feed.tms> <cmwell://meta/sys#indexTime> "1489414332875"^^<http://www.w3.org/2001/XMLSchema#long> .
        |<http://feed.tms> <cmwell://meta/sys#dataCenter> "vg" .
        |<http://feed.tms> <cmwell://meta/sys#lastModified> "1970-01-01T00:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        |<http://feed.tms> <cmwell://meta/sys#lastModifiedBy> "Baruch" .
        |<http://feed.tms> <cmwell://meta/sys#protocol> "http" .
        |<http://feed.tms> <cmwell://meta/sys#type> "ObjectInfoton" .
        |<http://feed.tms/1.0> <cmwell://meta/sys#indexTime> "1489414332875"^^<http://www.w3.org/2001/XMLSchema#long> .
        |<http://feed.tms/1.0> <cmwell://meta/sys#dataCenter> "vg" .
        |<http://feed.tms/1.0> <cmwell://meta/sys#lastModified> "1970-01-01T00:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        |<http://feed.tms/1.0> <cmwell://meta/sys#lastModifiedBy> "Baruch" .
        |<http://feed.tms/1.0> <cmwell://meta/sys#protocol> "http" .
        |<http://feed.tms/1.0> <cmwell://meta/sys#type> "ObjectInfoton" .
      """.stripMargin
    Http.post(_ow, payload, Some("text/nquads;charset=UTF-8"), List("format" -> "nquads"), tokenHeader).map{ res =>
      withClue(res) {
        res.status should be(200)
        Json.parse(res.payload) should be(jsonSuccess)
      }
    }
  }

  val waitForParents = ingestFeedTMS.flatMap { _ =>
    val waitForParentIngest1 = futureToWaitFor(Http.get(parent, List("op" -> "search", "length" -> "1", "format" -> "json")),1)
    val waitForParentIngest2 = futureToWaitFor(Http.get(cmw, List("op" -> "search", "qp" -> "system.path::/feed.tms", "length" -> "1", "format" -> "json")),1)
    for {
      r1 <- waitForParentIngest1
      r2 <- waitForParentIngest2
    } yield withClue(r1 -> r2)(succeed)
  }

  val emptyFile = {
    val payload =
    // scalastyle:off
      """
        |<http://la.grading.dev.thomsonreuters.com/badfile> <cmwell://meta/sys#indexTime> "1489414332875"^^<http://www.w3.org/2001/XMLSchema#long> .
        |<http://la.grading.dev.thomsonreuters.com/badfile> <cmwell://meta/sys#dataCenter> "blahlah" .
        |<http://la.grading.dev.thomsonreuters.com/badfile> <cmwell://meta/sys#lastModified> "1970-01-01T00:00:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
        |<http://la.grading.dev.thomsonreuters.com/badfile> <cmwell://meta/sys#lastModifiedBy> "Baruch" .
        |<http://la.grading.dev.thomsonreuters.com/badfile> <cmwell://meta/sys#protocol> "http" .
        |<http://la.grading.dev.thomsonreuters.com/badfile> <cmwell://meta/sys#type> "FileInfoton" .
      """.stripMargin
    // scalastyle:on
    Http.post(_ow, payload, Some("text/nquads;charset=UTF-8"), List("format" -> "nquads"), tokenHeader).map{ res =>
      withClue(res) {
        res.status should be(400)
      }
    }
  }

  describe("_ow API should") {
    it("reject empty file infoton")(emptyFile)
    it("ingest first version correctly")(firstIngest)
    it("make sure all children were indexed in ES")(childrenIndexed)
    it("make sure parent was NOT created with all children")(makeSureParentDirsWereNotCreated)
    it("make sure grand parent was NOT created")(makeSureGrandParentDirsWereNotCreated)
    it("succeed to ingest empty \"parent\" infotons")(ingestFeedTMS)
    it("verify parents were ingeseted")(waitForParents)
    //TODO: ingest all currents, make sure history is aligned
    //TODO: ingest a historic version, make sure all is good
  }
}
