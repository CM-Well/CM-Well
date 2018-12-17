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

package cmwell.irw

import java.net.URL

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.wait.strategy.Wait

import scala.io.Source

class KafkaTest2 extends FlatSpec with Matchers with ForAllTestContainer {
  override val container = GenericContainer("spotify/kafka",
    exposedPorts = Seq(80),
    waitStrategy = Wait.forLogMessage(".*kafka entered RUNNING state.*",1)
//    waitStrategy = Wait.forHttp("/")
  )

  "GenericContainer" should "start nginx and expose 80 port" in {
    assert(Source.fromInputStream(
      new URL(s"http://${container.containerIpAddress}:${container.mappedPort(80)}/").openConnection().getInputStream
    ).mkString.contains("If you see this page, the nginx web server is successfully installed"))
  }

}
