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


package cmwell.it


import cmwell.util.http.{SimpleResponse, SimpleResponseHandler}
import SimpleResponse.Implicits.UTF8StringHandler
import cmwell.util.concurrent.SimpleScheduler.scheduleFuture
import com.typesafe.scalalogging.LazyLogging
import org.scalatest
import org.scalatest.{AsyncFunSpec, Inspectors, Matchers, OptionValues}
import play.api.libs.json.{JsArray, JsDefined, JsString, Json}

import scala.concurrent.{ExecutionContext, Future, duration}
import duration.{DurationInt, FiniteDuration}

class ClientsCompatabilityTests extends AsyncFunSpec with OptionValues with Matchers with Helpers with fixture.NSHashesAndPrefixes with LazyLogging {

  val path = cmw / "clients.compatability.test.permid.org" / "timestamp"

  val ingestResponseToAssertion: SimpleResponse[String] => scalatest.Assertion = { res =>
    withClue(res) {
      res.status should be(200)
      Json.parse(res.payload) shouldEqual jsonSuccess
    }
  }

  val verifyResponseToAssertion: SimpleResponse[Array[Byte]] => scalatest.Assertion = { res =>
    withClue(res) {
      res.status should be(200)
      val JsDefined(JsArray(dates)) = Json.parse(res.payload) \ "fields" \ "hasIPODateB.organization" : @unchecked
      dates.headOption.value.as[String](play.api.libs.json.Reads.StringReads) should fullyMatch regex """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d+)?Z?"""

    }
  }

  val ingestSampleFromAnna1: Future[SimpleResponse[String]] = {
    val dateWithoutTZ = raw"""<http://clients.compatability.test.permid.org/timestamp/1-3133333333> <http://permid.org/ontology/organization/hasIPODateB> "2011-02-17 00:01:00.000"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."""
    Http.post(_in, dateWithoutTZ, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples", "force" -> ""), tokenHeader)
  }

  val verifySampleFromAnna1: Future[SimpleResponse[Array[Byte]]] = ingestSampleFromAnna1.flatMap { iRes =>
    spinCheck[Array[Byte]](1.second, true)(Http.get(path / "1-3133333333",Seq("format" -> "json")))(vRes => if (iRes.status != 400) vRes.status else iRes.status)
  }

  val ingestSampleFromAnna2: Future[SimpleResponse[String]] = {
    val dateWithoutT = raw"""<http://clients.compatability.test.permid.org/timestamp/1-3233333333> <http://permid.org/ontology/organization/hasIPODateB> "2011-02-17 00:01:00.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."""
    Http.post(_in, dateWithoutT, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples", "force" -> ""), tokenHeader)
  }

  val verifySampleFromAnna2: Future[SimpleResponse[Array[Byte]]] = ingestSampleFromAnna2.flatMap { iRes =>
    spinCheck[Array[Byte]](1.second, true)(Http.get(path / "1-3233333333",Seq("format" -> "json")))(vRes => if (iRes.status != 400) vRes.status else iRes.status)
  }

  val ingestSampleFromAnna3: Future[SimpleResponse[String]] = {
    val dateWithoutZ = raw"""<http://clients.compatability.test.permid.org/timestamp/1-3333333333> <http://permid.org/ontology/organization/hasIPODateB> "2011-02-17T00:01:00.000"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."""
    Http.post(_in, dateWithoutZ, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples", "force" -> ""), tokenHeader)
  }

  val verifySampleFromAnna3: Future[SimpleResponse[Array[Byte]]] = ingestSampleFromAnna3.flatMap { iRes =>
    spinCheck[Array[Byte]](1.second, true)(Http.get(path / "1-3333333333",Seq("format" -> "json")))(vRes => if (iRes.status != 400) vRes.status else iRes.status)
  }

  //NO MILLIS

  val ingestSampleFromAnna4: Future[SimpleResponse[String]] = {
    val dateWithoutTZ = raw"""<http://clients.compatability.test.permid.org/timestamp/1-3433333333> <http://permid.org/ontology/organization/hasIPODateB> "2011-02-17 00:01:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."""
    Http.post(_in, dateWithoutTZ, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples", "force" -> ""), tokenHeader)
  }

  val verifySampleFromAnna4: Future[SimpleResponse[Array[Byte]]] = ingestSampleFromAnna4.flatMap { iRes =>
    spinCheck[Array[Byte]](1.second, true)(Http.get(path / "1-3433333333",Seq("format" -> "json")))(vRes => if (iRes.status != 400) vRes.status else iRes.status)
  }

  val ingestSampleFromAnna5: Future[SimpleResponse[String]] = {
    val dateWithoutT = raw"""<http://clients.compatability.test.permid.org/timestamp/1-3533333333> <http://permid.org/ontology/organization/hasIPODateB> "2011-02-17 00:01:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."""
    Http.post(_in, dateWithoutT, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples", "force" -> ""), tokenHeader)
  }

  val verifySampleFromAnna5: Future[SimpleResponse[Array[Byte]]] = ingestSampleFromAnna5.flatMap { iRes =>
    spinCheck[Array[Byte]](1.second, true)(Http.get(path / "1-3533333333",Seq("format" -> "json")))(vRes => if (iRes.status != 400) vRes.status else iRes.status)
  }

  val ingestSampleFromAnna6: Future[SimpleResponse[String]] = {
    val dateWithoutZ = raw"""<http://clients.compatability.test.permid.org/timestamp/1-3633333333> <http://permid.org/ontology/organization/hasIPODateB> "2011-02-17T00:01:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."""
    Http.post(_in, dateWithoutZ, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples", "force" -> ""), tokenHeader)
  }

  val ingestSampleDateSingleSecondsDigit: Future[SimpleResponse[String]] = {
    val dateWithoutZ = raw"""<http://clients.compatability.test.permid.org/timestamp/1-3733333333> <http://permid.org/ontology/organization/hasIPODateB> "2017-04-18 11:30:00.0"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."""
    Http.post(_in, dateWithoutZ, Some("text/plain;charset=UTF-8"), List("format" -> "ntriples", "force" -> ""), tokenHeader)
  }
  val verifySampleDateSingleSecondsDigit: Future[SimpleResponse[Array[Byte]]] = ingestSampleDateSingleSecondsDigit.flatMap { iRes =>
    spinCheck[Array[Byte]](1.second, true)(Http.get(path / "1-3733333333",Seq("format" -> "json")))(vRes => if (iRes.status != 400) vRes.status else iRes.status)
  }


  val verifySampleFromAnna6: Future[SimpleResponse[Array[Byte]]] = ingestSampleFromAnna6.flatMap { iRes =>
    spinCheck[Array[Byte]](1.second, true)(Http.get(path / "1-3633333333",Seq("format" -> "json")))(vRes => if (iRes.status != 400) vRes.status else iRes.status)
  }

  describe("Clients compatability tests should") {
    it("ingest a date without 'T' & 'Z' succesfully")(ingestSampleFromAnna1.map(ingestResponseToAssertion))
    it("verify a date without 'T' & 'Z' was fixed succesfully")(verifySampleFromAnna1.map(verifyResponseToAssertion))
    it("ingest a date without 'T' succesfully")(ingestSampleFromAnna2.map(ingestResponseToAssertion))
    it("verify a date without 'T' was fixed succesfully")(verifySampleFromAnna2.map(verifyResponseToAssertion))
    it("ingest a date without 'Z' succesfully")(ingestSampleFromAnna3.map(ingestResponseToAssertion))
    it("verify a date without 'Z' was fixed succesfully")(verifySampleFromAnna3.map(verifyResponseToAssertion))
    it("ingest a date without millis & 'T' & 'Z' succesfully")(ingestSampleFromAnna4.map(ingestResponseToAssertion))
    it("verify a date without millis & 'T' & 'Z' was fixed succesfully")(verifySampleFromAnna4.map(verifyResponseToAssertion))
    it("ingest a date without millis & 'T' succesfully")(ingestSampleFromAnna5.map(ingestResponseToAssertion))
    it("verify a date without millis & 'T' was fixed succesfully")(verifySampleFromAnna5.map(verifyResponseToAssertion))
    it("ingest a date without millis & 'Z' succesfully")(ingestSampleFromAnna6.map(ingestResponseToAssertion))
    it("verify a date without millis & 'Z' was fixed succesfully")(verifySampleFromAnna6.map(verifyResponseToAssertion))
    it("ingest a date single milliseconds digit succesfully")(ingestSampleDateSingleSecondsDigit.map(ingestResponseToAssertion))
    it("verify a date with single milliseconds digit  was fixed succesfully")(verifySampleDateSingleSecondsDigit.map(verifyResponseToAssertion))
  }
}
