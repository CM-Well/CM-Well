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
package security.httpauth

import cmwell.util.string.Base64.decodeBase64String

trait BasicHttpAuthentication {
  private val authTypePrefix = "Basic "

  def decodeBasicAuth(auth: String): (String, String) = {
    val userColonPass = decodeBase64String(auth.drop(authTypePrefix.length), "UTF-8")
    val split = userColonPass.split(":", 2)
    if (split.length != 2)
      throw new IllegalArgumentException(s"`$auth` is not a valid Basic header")
    else
      split(0) -> split(1)
  }
}
