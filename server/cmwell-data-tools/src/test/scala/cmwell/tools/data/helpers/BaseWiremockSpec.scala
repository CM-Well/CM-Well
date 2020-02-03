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


package cmwell.tools.data.helpers

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import org.scalatest._

trait BaseWiremockSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach{
  val host = "localhost"
  val wireMockServer = new WireMockServer(wireMockConfig().dynamicPort())

  override def beforeAll(): Unit = {
    wireMockServer.start()
    WireMock.configureFor(host, wireMockServer.port)
    super.beforeAll()
  }

  override def afterEach: Unit = {
    WireMock.reset()
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    wireMockServer.shutdownServer()
    super.afterAll()
  }
}
