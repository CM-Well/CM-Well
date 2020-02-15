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


package cmwell.tracking

import cmwell.util.string.Base64.encodeBase64URLSafeString
import org.scalatest.{FlatSpec, Matchers, Succeeded}

/**
  * Created by yaakov on 3/15/17.
  */
class TrackingSpec extends FlatSpec with Matchers {
  "TrackingId encode decode" should "be successful" in {
    val actorId = "myAwesomeActor"
    val originalTid = TrackingId(actorId)

    originalTid.token match {
      case TrackingId(extractedTid) => extractedTid should equal(originalTid)
      case _ => Failed
    }
  }

  "Malformed token (not base64)" should "be None when unapplied" in
    assertNotValid("bla bla")

  "Malformed token (without createTime)" should "be None when unapplied" in
    assertNotValid(encodeBase64URLSafeString("id"))

  "Malformed token (malformed createTime)" should "be None when unapplied" in
    assertNotValid(encodeBase64URLSafeString("id|not-a-long"))

  private def assertNotValid(payload: String) = payload match {
    case TrackingId(_) => Failed
    case _ => Succeeded
  }
}
