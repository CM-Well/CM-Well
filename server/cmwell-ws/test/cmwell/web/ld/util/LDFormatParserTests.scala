/**
  * © 2020 Refinitiv. All Rights Reserved.
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
package cmwell.web.ld.util

import org.scalatest.{FlatSpec, Matchers}

class LDFormatParserTests extends FlatSpec with Matchers {
  "WildcardUrls" should "be validated properly" in {
    val validWildcardUrls = Seq(
      "/validWildcard/*",
      "/*",
      "/validWildcard#*",
      "#*",
      "/#*"
    ).map( "https://example.org" + _)
    val invalidWildcardUrls = Seq(
      "/invalid/*/wildcard",
      "/invalid#*/wildcard",
      "/**",
      "/*/",
      "/#*/"
    ).map( "https://example.org" + _)

    validWildcardUrls.foreach { LDFormatParser.isWildCardUrl(_) should be(true) }
    invalidWildcardUrls.foreach { LDFormatParser.isWildCardUrl(_) should be(false) }
  }
}
