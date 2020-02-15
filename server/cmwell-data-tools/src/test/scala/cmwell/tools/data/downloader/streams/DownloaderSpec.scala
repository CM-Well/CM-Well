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


package cmwell.tools.data.downloader.streams

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import org.scalatest._
import org.scalatest.prop._

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._

import scala.concurrent.Await
import scala.concurrent.duration._

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks


class DownloaderSpec extends PropSpec with ScalaCheckPropertyChecks with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("reactive-tools-system")
  implicit val mat: Materializer = ActorMaterializer()
  val host = "localhost"
  val numUuidsPerRequest = 25

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  property ("Download from uuids stream sends request blocks of uuids") {
    val uuids = Table(
      ("numUuids", "blocksToSend"),
      (1         , 1             ),
      (10        , 1             ),
      (20        , 1             ),
      (30        , 2             ),
      (60        , 3             )
    )

    forAll(uuids) { (numUuids: Int, expectedBlocksToSend: Int) =>

      // setup wiremock
      val wireMockServer = new WireMockServer(wireMockConfig().dynamicPort())
      wireMockServer.start()
      val port = wireMockServer.port
      WireMock.configureFor(host, port)

      // create sample uuids to download
      val data = for (x <- 1 to numUuids) yield s"uuid$x\n"

      // wiremock stubbing
      stubFor(post(urlPathMatching("/_out.*"))
        .willReturn(aResponse()
          .withBody("body")
          .withStatus(StatusCodes.OK.intValue)))

      // create uuid input-stream
      val in = Source.fromIterator(() => data.iterator)
        .map(ByteString.apply)
        .runWith(StreamConverters.asInputStream())

      // download mock data
      Await.result (
        Downloader.downloadFromUuidInputStream(
          baseUrl = s"$host:$port",
          numInfotonsPerRequest = numUuidsPerRequest,
          in = in)
        ,30.seconds
      )

      // verifying
      val numBlocksSent = findAll(postRequestedFor((urlPathMatching("/_out.*")))).size

      // teardown wiremock
      wireMockServer.shutdown()
      wireMockServer.stop()
      while (wireMockServer.isRunning) {}
      wireMockServer.resetRequests()

      numBlocksSent should be (expectedBlocksToSend)

    }
  }
}
