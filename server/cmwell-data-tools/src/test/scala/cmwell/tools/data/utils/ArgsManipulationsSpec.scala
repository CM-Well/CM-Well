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


package cmwell.tools.data.utils

import cmwell.tools.data.utils.ArgsManipulations._
import org.scalatest._

class ArgsManipulationsSpec extends FlatSpec with Matchers{
  "format host" should "add http prefix" in {
    val input = "my-string"
    val expected = s"http://$input"
    val result = formatHost(input)

    result should be (expected)
  }

  it should "not strip http prefix" in {
    val input = "https://my-string"
    val expected = input
    val result = formatHost(expected)

    result should be (expected)
  }

  "format path" should "add / prefix" in {
    val input = "my-string"
    val expected = "/" + input
    val result = formatPath(input)

    result should be ("/" + input)
  }

  it should "not add / prefix" in {
    val input = "/my-string"
    val result = formatPath(input)

    result should be (input)
  }

  "extract URL components" should "extract only host" in {
    val host = "api-garden"
    val result = extractBaseUrl(host)

    result should be (HttpAddress(host = host, port = 80))
  }

  it should "extract host and port" in {
    val host = "api-garden"
    val port = 8080

    val result = extractBaseUrl(s"$host:$port")
    result should be (HttpAddress(host = host, port = port))
  }

  it should "extract host and uri prefix" in {
    val host = "api-garden"
    val uriPrefix = "/uri/dummy"

    val result = extractBaseUrl(s"$host$uriPrefix")
    result should be (HttpAddress(host = host, port = 80, uriPrefix = uriPrefix))
  }

  it should "extract host, port and uri prefix" in {
    val host = "api-garden"
    val port = 8080
    val uriPrefix = "/uri/dummy"

    val result = extractBaseUrl(s"$host:$port$uriPrefix")
    result should be (HttpAddress(host = host, port = port, uriPrefix = uriPrefix))
  }
}
