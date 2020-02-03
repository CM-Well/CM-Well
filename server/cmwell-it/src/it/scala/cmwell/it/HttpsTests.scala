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
import org.scalatest.{Assertion, AsyncFunSpec, Inspectors, Matchers}
import play.api.libs.json.{JsDefined, JsNumber, Json}

import scala.concurrent.Future
import scala.concurrent.duration._

class HttpsTests extends AsyncFunSpec with Matchers with Helpers with Inspectors with LazyLogging {

  import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler

  describe("https: Graph Traversal") {
    val path = cmw / "example.org"

    val bName = """<https://example.org/B> <https://www.refinitiv-lbd.com/bold3/name> "My name is B" ."""
    val aName = """<https://example.org/A> <https://www.refinitiv-lbd.com/bold3/name> "My name is A" ."""
    val AandB = List(bName, aName)
    val allASons = List(
      """<https://example.org/A1> <https://www.refinitiv-lbd.com/bold3/name> "My name is A1" .""",
      """<https://example.org/A2> <https://www.refinitiv-lbd.com/bold3/name> "My name is A2" .""",
      """<https://example.org/A3> <https://www.refinitiv-lbd.com/bold3/name> "My name is A3" ."""
    )

    val dataIngest = {
      val data =
        """
          <https://example.org/A> <https://purl3.org/vocab/relationship3/predicate> <http://example.org/A1> .
          <https://example.org/A> <https://purl3.org/vocab/relationship3/predicate> <https://example.org/A2> .
          <https://example.org/A> <https://purl3.org/vocab/relationship3/predicate> <https://example.org/A3> .
          <https://example.org/A> <https://www.refinitiv-lbd.com/bold3/name> "My name is A" .
          <https://example.org/A1> <https://www.refinitiv-lbd.com/bold3/name> "My name is A1" .
          <https://example.org/A2> <https://www.refinitiv-lbd.com/bold3/name> "My name is A2" .
          <https://example.org/A3> <https://www.refinitiv-lbd.com/bold3/name> "My name is A3" .
          <https://example.org/B> <https://purl3.org/vocab/relationship3/predicate> <https://example.org/A> .
          <https://example.org/B> <https://www.refinitiv-lbd.com/bold3/name> "My name is B" .
      """.
          stripMargin

      Http.post(_in, data, queryParams = List("format" -> "ntriples"), headers = tokenHeader).flatMap { _ =>
        spinCheck(100.millis, true)(Http.get(path, List("op" -> "stream")))(
          _.payload.lines.count(path => path.contains("/A") || path.contains("/B")) >= 5
        )
      }
    }

    val verifyYgForB = dataIngest.flatMap { _ =>
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      spinCheck(100.millis, true)(Http.get(path / "B", List("yg" -> ">predicate.relationship3", "format" -> "ntriples"))) {
        res => {
          val resList = res.payload.lines.toList
          (res.status == 200) && AandB.forall(resList.contains)
        }
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          val resList = res.payload.lines.toList
          forAll(AandB) { l => resList.contains(l) should be(true) }
        }
      }
    }

    val verifyYgForA = dataIngest.flatMap { _ =>
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      spinCheck(100.millis, true)(Http.get(path / "A", List("yg" -> "<predicate.relationship3", "format" -> "ntriples"))) {
        res => {
          val resList = res.payload.lines.toList
          (res.status == 200) && AandB.forall(resList.contains)
        }
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          val resList = res.payload.lines.toList
          forAll(AandB) { l => resList.contains(l) should be(true) }
        }
      }
    }

    val verifyXgForA = dataIngest.flatMap { _ =>
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      spinCheck(100.millis, true)(
        Http.get(cmw, List("op" -> "search", "qp" -> "name.bold3::My name is A", "recursive" -> "", "with-data" -> "",
          "format" -> "ntriples", "xg" -> ""))) {
        res => {
          val resList = res.payload.lines.toList
          (res.status == 200) && (aName :: allASons).forall(resList.contains)
        }
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          val resList = res.payload.lines.toList
          forAll(aName :: allASons) { l => resList.contains(l) should be(true) }
        }
      }
    }

    val verifyGqpForPointingAtA = dataIngest.flatMap { _ =>
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      spinCheck(100.millis, true, 1.minute)(
        Http.get(cmw, List("op" -> "search", "qp" -> "name.bold3::My name is A", "recursive" -> "", "with-data" -> "",
          "gqp" -> """<predicate.relationship3[name.bold3::My name is B]""", "format" -> "ntriples"))) { res =>
        val resList = res.payload.lines.toList
        (res.status == 200) && resList.contains(aName)
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          res.payload.lines.toList should contain(aName)
        }
      }
    }

    val verifyGqpForAPointingAtHttp = dataIngest.flatMap { _ =>
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      spinCheck(100.millis, true, 1.minute)(
        Http.get(cmw, List("op" -> "search", "qp" -> "name.bold3::My name is A", "recursive" -> "", "with-data" -> "",
          "gqp" -> """>predicate.relationship3[name.bold3::My name is A1]""", "format" -> "ntriples"))) { res =>
        val resList = res.payload.lines.toList
        (res.status == 200) && resList.contains(aName)
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          res.payload.lines.toList should contain(aName)
        }
      }
    }

    val verifyGqpForAPointingAtHttps = dataIngest.flatMap { _ =>
      import cmwell.util.http.SimpleResponse.Implicits.UTF8StringHandler
      spinCheck(100.millis, true, 1.minute)(
        Http.get(cmw, List("op" -> "search", "qp" -> "name.bold3::My name is A", "recursive" -> "", "with-data" -> "",
          "gqp" -> """>predicate.relationship3[name.bold3::My name is A2]""", "format" -> "ntriples"))) { res =>
        val resList = res.payload.lines.toList
        (res.status == 200) && resList.contains(aName)
      }.map { res =>
        withClue(res) {
          res.status should be(200)
          res.payload.lines.toList should contain(aName)
        }
      }
    }

    val ingestDataForGhostInfotonsTest = Http.post(_in,
      """
        |<https://example.org/Y1> <https://purl3.org/vocab/relationship3/predicate> <https://example.org/Casper> .
        |<https://example.org/Y2> <https://purl3.org/vocab/relationship3/predicate> <https://example.org/Casper> .
      """.stripMargin, queryParams = "format" -> "ntriples" :: Nil,
      headers = tokenHeader).flatMap { _ =>
        spinCheck(100.millis, true)(Http.get(cmw / "example.org" / "Y1"))(_.status==200).zip(
        spinCheck(100.millis, true)(Http.get(cmw / "example.org" / "Y2"))(_.status==200)).zip(
        spinCheck(500.millis, true)(Http.get(cmw, List(
              "op"          -> "search",
              "qp"          -> "*path.system:/example.org/Y1,*path.system:/example.org/Y2",
              "recursive"   -> "",
              "format" -> "json"))){ r =>
            r.status == 200 && {
              val j = Json.parse(r.payload)
              (j \ "results" \ "total" : @unchecked) match {
                case JsDefined(JsNumber(bigDec)) => bigDec.intValue == 2
              }
            }
          }
        )

    }.flatMap { _ =>
      spinCheck(100.millis, true)(
        Http.get(cmw / "meta" / "ns", queryParams = Seq("op"->"stream", "qp"->"url::https://purl3.org/vocab/relationship3/"))
      )(
        _.payload.trim.lines.nonEmpty
      )
    }

    val ghostInfotonInMiddle = ingestDataForGhostInfotonsTest.flatMap { _ =>
      val p = "predicate.relationship3"
      Http.get(cmw / "example.org" / "Y1", Seq("yg" -> s">$p<$p", "format" -> "text")).map { r =>
        val results = r.payload.lines.toList
        withClue(results){ results.length should be(2) }
        forExactly(1, results)(_ should be("/example.org/Y1"))
        forExactly(1, results)(_ should be("/example.org/Y2"))
      }
    }

    val ghostInfotonInStart = ingestDataForGhostInfotonsTest.flatMap { _ =>
      val p = "predicate.relationship3"
      Http.get(cmw / "example.org" / "Casper", Seq("yg" -> s"<$p", "format" -> "text")).map { r =>
        val results = r.payload.lines.toList
        withClue(results){ results.length should be(2) }
        forExactly(1, results)(_ should be("/example.org/Y1"))
        forExactly(1, results)(_ should be("/example.org/Y2"))
      }
    }


    it("should verify YG > for B")(verifyYgForB)
    it("should verify YG < for A")(verifyYgForA)
    it("should verify XG for A")(verifyXgForA)
    it("should verify GQP > for A with HTTP relation")(verifyGqpForAPointingAtHttp)
    it("should verify GQP > for A with HTTPS relation")(verifyGqpForAPointingAtHttps)
    it("should verify GQP < for A")(verifyGqpForPointingAtA)
    it("should test GhostInfoton in the middle")(ghostInfotonInMiddle)
    it("should test starting with a GhostInfoton")(ghostInfotonInStart)
  }

  describe("https: RDF Preserves HTTPS protocol in Subjects") {
    def post(ntriples: String, replaceMode: Boolean = false) = {
      val queryParams = List("format" -> "ntriples") ++ (if(replaceMode) Some("replace-mode" -> "") else None).toList
      Http.post(_in, ntriples, queryParams = queryParams, headers = tokenHeader)
  }

    val path = cmw / "example7.org"
    val data =
      """
        |<https://example7.org/2Qvp0Ce43ZtC> <https://example7.org/ont777/prdct> <https://example7.org/WgESsdxaopZg> .
        |<https://example7.org/EgNZW3WbqsTx> <https://example7.org/ont777/prdct> <https://example7.org/lEVhjm2cIzo3> .
        |<https://example7.org/vOIlrqD0nA0Q> <https://example7.org/ont777/prdct> <https://example7.org/Wub6rvqJc7Q5> .
        |<https://example7.org/iqgPj4YQQRv0> <https://example7.org/ont777/prdct> <https://example7.org/kOVYZuGtdu1o> .
        |<https://example7.org/2UWyNftwMVH6> <https://example7.org/ont777/prdct> <https://example7.org/QjqRAEG1WtD8> .
        |<https://example7.org/iKVePWzwjnMA> <https://example7.org/ont777/prdct> <https://example7.org/lallbHWe3zss> .
        |<https://example7.org/RgyTXXjeeCEC> <https://example7.org/ont777/prdct> <https://example7.org/d5qMBVaYeiNf> .
        |<https://example7.org/5VoOejsG72df> <https://example7.org/ont777/prdct> <https://example7.org/DgGUFi3B8kfK> .
        |<https://example7.org/efCU5fU3dL5S> <https://example7.org/ont777/prdct> <https://example7.org/VlbLAo87d5GT> .
        |<https://example7.org/G5LnNJozRsbh> <https://example7.org/ont777/prdct> <https://example7.org/T5ZnQwh3Nd66> .
      """.stripMargin

    val ingest = post(data).flatMap( _ =>
      spinCheck(100.millis, true)(Http.get(path, List("op" -> "stream")))(_.payload.lines.length >= 10)
    )

    val inAndOut = {
      ingest.flatMap { _ =>
        Http.get(path, List("op" -> "stream", "format" -> "ntriples")).map(_.payload.lines).
          map(triples => forAll(triples.toList)(_ should startWith("<https")))
      }
    }

    def versions(replaceMode: Boolean): Future[Assertion] = {

      // version1: https
      // version2: http
      // version3: https

      val path = cmw / "example9.org" / (if(replaceMode) "protocol-versioning-rm" else "protocol-versioning")
      def version(protocol: String, version: Int) =
        s"""<$protocol://example9.org/protocol-versioning${if(replaceMode)"-rm" else ""}> <https://example9.org/ont999/prdct> "v$version" ."""

      def validateMetaNs = if(replaceMode)
        spinCheck(100.millis, true)(
          Http.get(cmw / "meta" / "ns", queryParams = Seq("op"->"stream", "qp"->"url::https://example9.org/ont999/"))
        )(
          _.payload.trim.lines.nonEmpty
        ).map(_ => ()) else Future.successful(())

      val firstVersion = post(version("https", 1)).flatMap { _ =>
        validateMetaNs.flatMap { _ =>
          spinCheck(100.millis, true)(Http.get(path, queryParams = "format" -> "ntriples" :: Nil))(_.payload.contains("v1")).
            map(_.payload should startWith("<https:"))
        }
      }

      val secondVersion = firstVersion.flatMap{ _ =>
        post(version("http", 2), replaceMode = replaceMode).flatMap{ _ =>
          spinCheck(100.millis, true)(Http.get(path, queryParams = "format"->"ntriples" :: Nil))(_.payload.contains("v2")).
            map(_.payload should startWith("<http:"))
        }
      }

      secondVersion.flatMap { _ =>
        post(version("https", 3), replaceMode = replaceMode).flatMap{ _ =>
          spinCheck(100.millis, true)(Http.get(path, queryParams = "format"->"ntriples" :: Nil))(_.payload.contains("v3")).
            map(_.payload should startWith("<https:"))
        }
      }
    }

    val _sp = {
      val spBody =
        s"""
          |PATHS
          |/example7.org?op=search&with-data
          |
          |SPARQL
          |CONSTRUCT { ?s ?p ?o . } WHERE { ?s ?p ?o }
        """.stripMargin
      ingest.flatMap { _ =>
        Http.post(cmw / "_sp", spBody).map(_.payload.trim.lines).
          map(triples => forAll(triples.toList)(_ should startWith("<https:")))
      }
    }

    val nullUpdate = {
      val path = cmw / "example777.org" / "null-update"
      val data = (protocol:String) => s"""<$protocol://example777.org/null-update> <https://example7.org/ont777/prdct> "same value" ."""
      post(data("http")).flatMap { _ => spinCheck(100.millis, true)(Http.get(path))(_.status == 200) }.flatMap { _ =>
       post(data("https")).flatMap { _ =>
         spinCheck(100.millis, true)(Http.get(path, queryParams = Seq("with-history" -> "", "format" -> "ntriples")))(
           _.payload.lines.count(line => !line.contains("meta/sys")) == 2
         ).map { resp =>
           val historicalVersions = resp.payload.lines.filterNot(_.contains("meta/sys")).toList
           historicalVersions.length should be(2)
           forExactly(1, historicalVersions)(_ should startWith("<http:"))
           forExactly(1, historicalVersions)(_ should startWith("<https:"))
         }
       }
      }
    }

    val search = {
      ingest.flatMap{ _ =>
          spinCheck(100.millis, true)(Http.get(path, queryParams = Seq("op"->"stream", "qp"->"system.protocol::https")))(
            _.payload.nonEmpty).flatMap(_.payload shouldNot be(empty)
          )
        }
      }

    val nonTopDomain = {
      post("""<https://rubber/duck/debugger> <https://purl73.org/vocab/relationship73/predicate> "BE HERE NOW". """).flatMap { _ =>
        spinCheck(100.millis, true)(Http.get(cmw / "rubber" / "duck" / "debugger", "format" -> "ntriples" :: Nil))(_.status==200).map { r =>
          forAtLeast(1, r.payload.lines.toList)(_ should startWith("<https:"))
        }
      }
    }


    it("should preserve https protocol from ingest to _out")(inAndOut)
    it("should change protocol in each ingested version accordingly")(versions(replaceMode = false))
    it("should change protocol in each ingested version accordingly, but with replace-mode")(versions(replaceMode = true))
    it("should get https data when using _sp API")(_sp)
    it("should allow changing protocol upon a Null Update")(nullUpdate)
    it("should support search protocol system field")(search)
    it("should support https in non top-domain paths")(nonTopDomain)
  }

  describe("Changing protocol without changing data") {
    def blockingPost(ntriples: String) =
      Http.post(_in, ntriples, queryParams = List("format" -> "ntriples", "blocking" -> ""), headers = tokenHeader)

    val nonExistingInfoton: Future[Assertion] = {
      val payload = """<https://example.org/no-such-infoton-in-the-world> <cmwell://meta/sys#path> "/example.org/no-such-infoton-in-the-world" . """
      blockingPost(payload).flatMap { _ =>
        Http.get(cmw / "example.org" / "no-such-infoton-in-the-world").flatMap(_.status should be(404))
      }
    }

    it("should keep non-existing Infoton 404 even when changing its protocol")(nonExistingInfoton)
  }
}
