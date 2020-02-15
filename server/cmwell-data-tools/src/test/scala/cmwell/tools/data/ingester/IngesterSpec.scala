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


package cmwell.tools.data.ingester

import java.io._
import java.util.zip.GZIPOutputStream

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import cmwell.tools.data.helpers.BaseWiremockSpec
import cmwell.tools.data.utils.akka.HeaderOps
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.stubbing.Scenario
import org.scalatest.Ignore

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Created by matan on 7/4/16.
  */
class IngesterSpec extends BaseWiremockSpec  {
  implicit val system: ActorSystem = ActorSystem("reactive-tools-system")
  implicit val mat: Materializer = ActorMaterializer()
  val path = "example.org"

  val SCENARIO = "test retry"
  val SUCCESS_STATE = "success-state"
  val FAIL_STATE = "fail-state"

  override protected def afterAll(): Unit = {
//    wireMockServer.shutdown()
    system.terminate()
    super.afterAll()
  }

  def createTriplesFile(numInfotons: Integer, numTriples: Integer, filename: String, zipped: Boolean = false) = {
    def subject(x: Integer)   = s"<http://$path/subject-$x>"
    def predicate(x: Integer) = s"<http://$path.org/predicate#p$x>"
    def obj(x: Integer)       = s""""${x}"^^<http://www.w3.org/2001/XMLSchema#string> ."""

//    val writer = new PrintWriter(testFile)

    val file = new BufferedOutputStream(new FileOutputStream(filename))
    val writer = if (zipped) new PrintWriter(new GZIPOutputStream(file)) else new PrintWriter(file)

    for {
      i <- 1 to numInfotons
      t <- 1 to numTriples
    } writer.println(s"${subject(i)} ${predicate(t)} ${obj(t)}")

    writer.flush()
    writer.close()
  }

  def createTriplesStream(numInfotons: Integer, numTriples: Integer) = {
    def subject(x: Integer)   = s"<http://$path/subject-$x>"
    def predicate(x: Integer) = s"<http://$path.org/predicate#p$x>"
    def obj(x: Integer)       = s""""${x}"^^<http://www.w3.org/2001/XMLSchema#string> ."""

    Iterator.range(1, numInfotons + 1)
      .flatMap(i => (1 to numTriples).map(t => s"${subject(i)} ${predicate(t)} ${obj(t)}\n"))
  }

  private def numInfotonsInServer = {
    val port = wireMockServer.port
    val req = HttpRequest(uri = s"http://$host:$port/$path/?op=stream&length=1")
    val numRecordsFuture = Http().singleRequest(req)
      .map(response => HeaderOps.getNumInfotons(response.headers))

    val headerFuture = numRecordsFuture.map ( _ match {
        case None => ???
        case Some(numRecords) => numRecords
      }
    )

    val header = Await.result(headerFuture, Duration.Inf)

    header.value.toLong
  }

  "Ingester" should "ingest all blocks without retries" in {
    val numInfotonsToCreate = 30

    stubFor(post(urlPathMatching("/_in"))
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)))

    val in = Source.fromIterator(() => createTriplesStream(numInfotonsToCreate, 10))
      .map(ByteString.apply)
      .runWith(StreamConverters.asInputStream())

    val result = Ingester
      .fromInputStream(s"$host:${wireMockServer.port}", "ntriples", in = in)
      .runWith(Sink.ignore)

    result.flatMap { _ =>
      wireMockServer.findAll(postRequestedFor(urlPathMatching("/_in"))).size should be (2)
    }
  }

  it should "split data into single infotons (client error) and succeed ingest" in {
    Ingester.retryTimeout = 1.seconds

    val numInfotonsToCreate = 10

    stubFor(post(urlPathMatching("/_in")).inScenario(SCENARIO)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.NotFound.intValue))
      .willSetStateTo(SUCCESS_STATE))

    stubFor(post(urlPathMatching("/_in")).inScenario(SCENARIO)
      .whenScenarioStateIs(SUCCESS_STATE)
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)))

    val in = Source.fromIterator(() => createTriplesStream(numInfotonsToCreate, 10))
      .map(ByteString.apply)
      .runWith(StreamConverters.asInputStream())

    val result = Ingester
      .fromInputStream(s"$host:${wireMockServer.port}", "ntriples", in = in)
      .runWith(Sink.ignore)

    result.flatMap { _ =>
      wireMockServer.findAll(postRequestedFor(urlPathMatching("/_in"))).size should be (numInfotonsToCreate + 1)
    }
  }

  it should "ingest first block and retry second block (client error)" in {
    Ingester.retryTimeout = 1.seconds

    val numInfotonsToCreate = 300

    stubFor(post(urlPathMatching("/_in")).inScenario(SCENARIO)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue))
      .willSetStateTo(FAIL_STATE))

    stubFor(post(urlPathMatching("/_in")).inScenario(SCENARIO)
      .whenScenarioStateIs(FAIL_STATE)
      .willReturn(aResponse()
        .withStatus(StatusCodes.NotFound.intValue))
      .willSetStateTo(SUCCESS_STATE))

    stubFor(post(urlPathMatching("/_in")).inScenario(SCENARIO)
      .whenScenarioStateIs(SUCCESS_STATE)
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)))

    val in = Source.fromIterator(() => createTriplesStream(numInfotonsToCreate, 10))
      .map(ByteString.apply)
      .runWith(StreamConverters.asInputStream())

    val result = Ingester
      .fromInputStream(baseUrl = s"$host:${wireMockServer.port}", "ntriples", in = in)
      .runWith(Sink.ignore)

    result.flatMap { _ =>
      val requestsReceived = wireMockServer.findAll(postRequestedFor(urlPathEqualTo("/_in")))
      requestsReceived.size should be > 30
    }
  }

  it should "retry packet twice (server error) and then succeed ingesting" in {
    Ingester.retryTimeout = 1.seconds

    val in = Source.fromIterator(() => createTriplesStream(1, 1))
      .map(ByteString.apply)
      .runWith(StreamConverters.asInputStream())

    stubFor(post(urlPathMatching("/_in")).inScenario(SCENARIO)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.ServiceUnavailable.intValue))
      .willSetStateTo(FAIL_STATE)
    )

    stubFor(post(urlPathMatching("/_in")).inScenario(SCENARIO)
      .whenScenarioStateIs(FAIL_STATE)
      .willReturn(aResponse()
        .withStatus(StatusCodes.ServiceUnavailable.intValue))
      .willSetStateTo(SUCCESS_STATE)
    )

    stubFor(post(urlPathMatching("/_in")).inScenario(SCENARIO)
      .whenScenarioStateIs(SUCCESS_STATE )
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)))

    val result = Ingester.fromInputStream(
      baseUrl = s"$host:${wireMockServer.port}",
      format = "ntriples",
      in = in)
    .runWith(Sink.ignore)

    result.flatMap { _ =>
      wireMockServer.findAll(postRequestedFor(urlPathMatching("/_in"))).size should be (3)
    }
  }

  it should "split packet to single infotons (client error), do retries and fail ingest" in {
    val numInfotonsToIngest = 10

    Ingester.retryTimeout = 1.seconds

    val in = Source.fromIterator(() => createTriplesStream(numInfotonsToIngest, 1))
      .map(ByteString.apply)
      .runWith(StreamConverters.asInputStream())

    stubFor(post(urlPathMatching("/_in"))
      .willReturn(aResponse()
        .withStatus(StatusCodes.NotFound.intValue))
    )

    val result = Ingester.fromInputStream(
      baseUrl = s"$host:${wireMockServer.port}",
      format = "ntriples",
      in = in)
      .runWith(Sink.ignore)

    result.flatMap { _ =>
      wireMockServer.findAll(postRequestedFor(urlPathMatching("/_in"))).size should be (numInfotonsToIngest + 1)
    }
  }
}
