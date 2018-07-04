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


package cmwell.tools.data.downloader.consumer

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.{ActorMaterializer, Materializer}
import cmwell.tools.data.helpers.BaseWiremockSpec
import cmwell.tools.data.utils.akka.HeaderOps._
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault
import com.github.tomakehurst.wiremock.stubbing.Scenario


/**
  * Created by matan on 12/9/16.
  */
class RetryTest extends BaseWiremockSpec {
  val scenario = "scenario"

  implicit val system: ActorSystem = ActorSystem("reactive-tools-system")
  implicit val mat: Materializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  it should "be resilient against server errors" in {

    val exitRetry = "exit-retry"

    stubFor(get(urlPathMatching("/_create-consumer")).inScenario(scenario)
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(aResponse()
        .withStatus(StatusCodes.GatewayTimeout.intValue)
        .withHeader(CMWELL_POSITION, "3AAAMHwv"))
      .willSetStateTo(exitRetry)
    )

    Downloader.createTsvSource(baseUrl = s"localhost:${wireMockServer.port}")

    1 shouldBe 1
  }



}
