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

import java.util.concurrent.Executors

import cmwell.tracking.{Failed, InProgress, PathStatus}
import cmwell.util.concurrent.{retry, unsafeRetryUntil}
import cmwell.util.http.SimpleResponse
import com.typesafe.scalalogging.LazyLogging
import org.scalatest._
import play.api.libs.json.Json

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Created by yaakov on 3/20/17.
  */
class TrackingTests extends AsyncFunSpec with Matchers with Helpers with OptionValues with Inspectors with LazyLogging {

  import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler

  private val _track = cmw / "_track"
  private val ingestQueryParams = List("format" -> "ntriples")
  private val trackingQueryParams = ingestQueryParams :+ "tracking" -> ""
  private val blockingQueryParams = ingestQueryParams :+ "blocking" -> ""

  private val possibleStatuses = Set("InProgress", "Done", "Evicted", "Failed")

  private val statusPredicate = s"${cmw.url}/meta/nn#trackingStatus"
  private val prevUuidPredicate = "cmwell://meta/sys#prevUUID"

  val sanity: Future[Assertion] = {
    val subjects = toSubjects(List("s1", "s2", "s3", "s4"))
    val ntriples = toNtriples(subjects)

    ingestAndGetTrackingId(ntriples.mkString("\n")).flatMap { tid =>
      tid shouldBe defined
      Http.get(_track / tid.value).map { trackResponse =>
        val responseNtriples = trackResponse.payload.split('\n')

        val responseSubjects = responseNtriples.map(_.takeWhile(_ != ' '))
        responseSubjects should contain theSameElementsAs subjects

        every(responseNtriples) should include regex possibleStatuses.mkString("|")
      }
    }
  }

  val waitUntilDone: Future[Assertion] = {
    val payload = toNtriples(toSubjects(List("s5", "s6", "s7", "s8"))).mkString("\n")
    ingestAndGetTrackingId(payload).flatMap { tid =>
      tid shouldBe defined
      retry(10, 5.seconds)(
        Http.get(_track / tid.value).map { resp =>
          withClue(resp) {
            resp.headers should contain("X-CM-WELL-TRACKING" -> "Done")
          }
        })
    }
  }

  val invalid: Future[Assertion] = Http.get(_track / "yada-yada-yada").map(_.status should be(400))

  val gone: Future[Assertion] =
    Http.get(_track / "XwAAZ29uZVRlc3RBY3RvcnwxNDkwMTMwNDg1ODA1").map(_.status should be(410))

  val resurrected: Future[Assertion] = {
    val tid = "1gAAdGVzdEFjdG9yfDE0OTAxMzA0ODU4MDU" // encodeBase64URLSafeString(compress("testActor|1490130485805"))

    val r1 = "/tracking.tests.com/r1"
    val r2 = "/tracking.tests.com/r2"

    val data = Seq(PathStatus(r1, InProgress), PathStatus(r2, Failed))

    val expectedResults = Seq(
      s"""<http:/$r1> <$statusPredicate> "InProgress" .""",
      s"""<http:/$r2> <$statusPredicate> "Failed" ."""
    )

    simulateDeadTrackingActor("testActor", data).flatMap { _ =>
      Http.get(_track / tid).map(_.payload.split('\n') should contain theSameElementsAs expectedResults)
    }
  }

  val resurrectedWithDirtyData: Future[Assertion] = {
    implicit val ex = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(5))
    val tid = "CAAAdGVzdEFjdG9yMnwxNDkwMjYyMzkwNTYx" // encodeBase64URLSafeString(compress("testActor2|1490262390561"))

    val rd1Path = "/tracking.tests.com/rd1"
    val rd1Url = cmw / "tracking.tests.com" / "rd1"
    val ntriples = toNtriples(toSubjects(Seq("rd1"))).mkString("\n")

    // expected is Done, even actor contains InProgress - that's dirty data.
    // test should succeed because Infoton's lastModified (i.e. DateTime.now) isAfter 1490262390561
    val expectedResults = s"""<http:/$rd1Path> <$statusPredicate> "Done" .\n"""

    val ingestInfotonAndWaitUntilItIsPersisted = Http.post(_in, ntriples, textPlain, trackingQueryParams, tokenHeader).flatMap { resp =>
        jsonSuccessPruner(Json.parse(resp.payload)) shouldBe jsonSuccess
        def ready(resp: SimpleResponse[String]) = resp.status == 200 && resp.payload.split("\n").exists(_.contains("indexTime"))
        unsafeRetryUntil[SimpleResponse[String]](ready,10,500.millis)(Http.get(rd1Url, List("format"->"ntriples"))).map(_ => ())
      }
    val simulateDeadActor: Future[Int] = simulateDeadTrackingActor("testActor2", Seq(PathStatus(rd1Path, InProgress)))

    ingestInfotonAndWaitUntilItIsPersisted.zip(simulateDeadActor).flatMap{ case (_, simulateStatus) =>
      simulateStatus shouldBe 200
      val sc = spinCheck(500 milliseconds)(Http.get(_track / tid)){_.payload == expectedResults}
      sc.map(_.payload should equal(expectedResults))
    }
  }

  val makeSureNoMetaNsAreTracked: Future[Assertion] = {
    val subjects = toSubjects(List("s9", "s10"))
    val predicateOfNewNamespace = "http://tracking-new-namespace.tests.com/tracking-new/track"
    val ntriples = subjects.map(s => s"""$s <$predicateOfNewNamespace> "a value" .""")

    ingestAndGetTrackingId(ntriples.mkString("\n")).flatMap { tid =>
      tid shouldBe defined
      Http.get(_track / tid.value).map { trackResponse =>
        val responseNtriples = trackResponse.payload.split('\n')

        val responseSubjects = responseNtriples.map(_.takeWhile(_ != ' '))
        responseSubjects should contain theSameElementsAs subjects

        every(responseNtriples) shouldNot include regex "meta/ns"
      }
    }
  }

  val blocking: Future[Assertion] = {
    val subject = "s11"
    val subjects = toSubjects(List(subject))
    val ntriples = toNtriples(subjects)
    Http.post(_in, ntriples.mkString("\n"), textPlain, blockingQueryParams, tokenHeader).flatMap { ingestResponse =>
      ingestResponse.payload.trim.split("\n") should contain theSameElementsAs subjects.map(s => s"""$s <$statusPredicate> "Done" .""")
      Http.get(cmw / "tracking.tests.com" / subject, Seq("format"->"ntriples")).map(_.status shouldBe 200)
    }
  }

  val atomic: Future[Assertion] = {
    val subject = "s12"
    val subjects = toSubjects(List(subject))
    val ntriples = toNtriples(subjects)
    Http.post(_in, ntriples.mkString("\n"), textPlain, blockingQueryParams, tokenHeader).flatMap { _ =>
      Http.get(cmw / "tracking.tests.com" / subject, Seq("format"->"json")).flatMap(exrtactUuidFromJsonRespone _ andThen { existingUuid =>
        val otherUuid = "a"*32
        val newPayload =
          s"""${subjects.head} <http://tracking.tests.com/tracking/track1> "new value" .
             |${subjects.head} <$prevUuidPredicate> "$otherUuid" .""".stripMargin
        val expectedResults = s"""${subjects.head} <$statusPredicate> "Evicted(expected:$otherUuid,actual:$existingUuid)" ."""
        ingestAndGetTrackingId(newPayload).flatMap { tid =>
          tid shouldBe defined
          val actualFut = unsafeRetryUntil((resp: String) => !resp.contains("InProgress"), 5, 1.second)(Http.get(_track / tid.get).map(_.payload.trim))
          actualFut.map(_ should be(expectedResults))
        }
      })
    }
  }

  it("should ingest data (with tracking flag), receive TID, query with it and get progress statuses")(sanity)
  it("should ingest data and wait until its progress is done")(waitUntilDone)
  it("should get 400 Bad Request for invalid TID")(invalid)
  it("should get 410 Gone for non-existing TID")(gone)
  it("should be able to resurrect an actor from zstore")(resurrected)
  it("should be able to resurrect an actor from zstore with dirty data discovery")(resurrectedWithDirtyData)
  it("should confirm no /meta/ns Infotons are tracking (in case of new namespace in data)")(makeSureNoMetaNsAreTracked)

  // Blocking
  it("should be able to block until processing is done (Blocking API)")(blocking)

  //Atomic Versioning
  it("should evict a version (Atomic Versioning)")(atomic)

  private def toSubjects(names: Seq[String]): Seq[String] =
    names.map(s => s"<http://tracking.tests.com/$s>")

  private def toNtriples(subjects: Seq[String], value: Option[String] = None): Seq[String] =
    subjects.map(s => s"""$s <http://tracking.tests.com/tracking/track> "${value.getOrElse("a value")}" .""")

  private def ingestAndGetTrackingId(ntriples: String): Future[Option[String]] = {
    Http.post(_in, ntriples, textPlain, trackingQueryParams, tokenHeader).map { ingestResponse =>
      jsonSuccessPruner(Json.parse(ingestResponse.payload)) should be(jsonSuccess)
      ingestResponse.headers.find(_._1 == "X-CM-WELL-TID").map(_._2)
    }
  }

  private def simulateDeadTrackingActor(actorName: String, data: Seq[PathStatus]): Future[Int] = {
    val uzid = s"ta-$actorName"
    val payload = data.map{ case PathStatus(p,s) => s"${p}\u0000$s" }.mkString("\n").getBytes("UTF-8")
    Http.post(cmw / "zz" / uzid, payload, headers = tokenHeader).map(_.status)
  }

  private def exrtactUuidFromJsonRespone(resp: SimpleResponse[String]): String =
    (Json.parse(resp.payload) \ "system" \ "uuid").as[String]
}
