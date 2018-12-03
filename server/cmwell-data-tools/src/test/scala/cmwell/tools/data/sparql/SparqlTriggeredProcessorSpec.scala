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



package cmwell.tools.data.sparql

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.{GraphDSL, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitches, Materializer}
import cmwell.tools.data.downloader.consumer.Downloader.Token
import cmwell.tools.data.helpers.BaseWiremockSpec
import cmwell.tools.data.utils.akka.HeaderOps.CMWELL_POSITION
import cmwell.tools.data.utils.akka._
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.matching._
import com.github.tomakehurst.wiremock.matching.EqualToPattern
import com.github.tomakehurst.wiremock.stubbing.Scenario
import org.scalatest.{Ignore, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._
import collection.JavaConverters._

@Ignore class SparqlTriggeredProcessorSpec extends BaseWiremockSpec {
  val scenario = "scenario"
  val `no-content` = "no-content"
  val `new-data-update` = "new-data-update"

  implicit val system: ActorSystem = ActorSystem("reactive-tools-system")
  implicit val mat: Materializer = ActorMaterializer()

  val path1 = "/path1/subject-1\tlastModified1-1\tuuid1-1"
  val path2 = "/path2/subject-2\tlastModified2-1\tuuid2-1"

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "Sparql Triggered Processor" should "receive materialized value detected by a sensor" in {

    val materializedPath1 = Seq("<http://subject-1> <predicate> object .",
                                "<http://subject-2> <predicate> object .")

    stubFor(get(urlMatching("/.*op=create-consumer.*"))
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv"))
    )

    stubFor(get(urlPathEqualTo("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv")
        .withBody(path1)
    ).willSetStateTo(`no-content`))

    stubFor(get(urlPathEqualTo("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(`no-content`)
      .willReturn(aResponse()
        .withStatus(StatusCodes.NoContent.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv")
      )
    )

    stubFor(post(urlPathEqualTo("/_sp"))
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withBody(materializedPath1.mkString("\n")
      )))

    val config = Config(
      sensors = Seq(
        Sensor(name = "sensor1", path ="/path1")
      ),
      useQuadsInSp = Some(false),
      updateFreq = 10.seconds,
      sparqlMaterializer = "",
      hostUpdatesSource = Some("http://localhost:9000")
    )

    val baseUrl = s"localhost:${wireMockServer.port}"

    val (killSwitch, result) = SparqlTriggeredProcessor.listen(config = config, baseUrl = baseUrl, useQuadsInSp = false)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.fold(blank){case (agg, (line, _)) => agg ++ line ++ endl})(Keep.both)
      .run()


    Thread.sleep(20000)
    killSwitch.shutdown()

    result
      .flatMap { materializedValue =>
        materializedValue.utf8String should be (materializedPath1.mkString("", "\n", "\n"))

        val createConsumerRequests = wireMockServer.findAll(postRequestedFor(urlPathMatching("/.*op=create-consumer.*"))).asScala
        createConsumerRequests.forall(_.getHeader("User-Agent") == "cmwell-data-tools sensor1") should be (true)

        val SpRequests = wireMockServer.findAll(postRequestedFor(urlPathMatching("/_sp"))).asScala
        SpRequests.forall(_.getHeader("User-Agent") == "cmwell-data-tools sparql-materializer") should be (true)
        SpRequests should have size (1)
      }
  }

  it should "materialized only unique paths in a timed window" in {
    val materializedPath1 = Seq("<http://subject-1> <predicate> object .",
                                "<http://subject-2> <predicate> object .")

    stubFor(get(urlMatching("/.*op=create-consumer.*"))
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv"))
    )

    stubFor(get(urlPathEqualTo("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv")
        .withBody(path1)
      ).willSetStateTo(`no-content`))

    stubFor(get(urlPathEqualTo("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(`no-content`)
      .willReturn(aResponse()
        .withStatus(StatusCodes.NoContent.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv")
      )
    )

    stubFor(post(urlPathEqualTo("/_sp"))
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withBody(materializedPath1.mkString("\n")
        )))

    val config = Config(
      sensors = Seq(
        Sensor(name = "sensor1", path ="/path1"),
        Sensor(name = "sensor2", path ="/path1")
      ),
      useQuadsInSp = Some(false),
      updateFreq = 10.seconds,
      sparqlMaterializer = "",
      hostUpdatesSource = Some("http://localhost:9000")
    )

    val baseUrl = s"localhost:${wireMockServer.port}"

    val (killSwitch, result) = SparqlTriggeredProcessor.listen(config = config, baseUrl = baseUrl, useQuadsInSp = false)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.fold(blank){case (agg, (line, _)) => agg ++ line ++ endl})(Keep.both)
      .run()


    Thread.sleep(20000)
    killSwitch.shutdown()

    result
      .flatMap { materializedValue =>
        materializedValue.utf8String should be (materializedPath1.mkString("", "\n", "\n"))

        val createConsumerRequests = wireMockServer.findAll(postRequestedFor(urlPathMatching("/.*op=create-consumer.*"))).asScala
        createConsumerRequests.forall(_.getHeader("User-Agent") == "cmwell-data-tools sensor1") should be (true)

        val SpRequests = wireMockServer.findAll(postRequestedFor(urlPathMatching("/_sp"))).asScala
        SpRequests.forall(_.getHeader("User-Agent") == "cmwell-data-tools sparql-materializer") should be (true)
        SpRequests should have size (1)
      }
  }

  it should "poll for new data updates" in {
    val materializedPath1 = Seq("<http://subject-1> <predicate> object .",
                                "<http://subject-2> <predicate> object .")

    stubFor(get(urlMatching("/.*op=create-consumer.*"))
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv"))
    )

    stubFor(get(urlPathEqualTo("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv")
        .withBody(path1)
      ).willSetStateTo(`no-content`))

    stubFor(get(urlPathEqualTo("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(`no-content`)
      .willReturn(aResponse()
        .withStatus(StatusCodes.NoContent.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv")
      ).willSetStateTo(`new-data-update`)
    )

    stubFor(get(urlPathEqualTo("/_consume")).inScenario(scenario)
      .whenScenarioStateIs(`new-data-update`)
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv")
        .withBody(path1)
      ).willSetStateTo("done")
    )

    stubFor(get(urlPathEqualTo("/_consume")).inScenario(scenario)
      .whenScenarioStateIs("done")
      .willReturn(aResponse()
        .withStatus(StatusCodes.NoContent.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv")
      )
    )

    stubFor(post(urlPathEqualTo("/_sp"))
      .willReturn(aResponse()
        .withStatus(StatusCodes.OK.intValue)
        .withBody(materializedPath1.mkString("\n")
        )))

    val config = Config(
      sensors = Seq(
        Sensor(name = "sensor1", path ="/path1")
      ),
      useQuadsInSp = Some(false),
      updateFreq = 5.seconds,
      sparqlMaterializer = "",
      hostUpdatesSource = Some("http://localhost:9000")
    )

    val baseUrl = s"localhost:${wireMockServer.port}"

    val (killSwitch, result) = SparqlTriggeredProcessor.listen(config = config, baseUrl = baseUrl, distinctWindowSize = 2.seconds, useQuadsInSp = false)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.fold(blank){case (agg, (line, _)) => agg ++ line ++ endl})(Keep.both)
      .run()


    Thread.sleep(20000)
    killSwitch.shutdown()

    result
      .flatMap { materializedValue =>
        materializedValue.utf8String should be ((materializedPath1 ++ materializedPath1).mkString("", "\n", "\n"))

        val createConsumerRequests = wireMockServer.findAll(postRequestedFor(urlPathMatching("/.*op=create-consumer.*"))).asScala
        createConsumerRequests.forall(_.getHeader("User-Agent") == "cmwell-data-tools sensor1") should be (true)

        val SpRequests = wireMockServer.findAll(postRequestedFor(urlPathMatching("/_sp"))).asScala
        SpRequests.forall(_.getHeader("User-Agent") == "cmwell-data-tools sparql-materializer") should be (true)
        SpRequests should have size (2)
      }

  }
}
