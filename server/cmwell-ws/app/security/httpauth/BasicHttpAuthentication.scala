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


package security.httpauth

import org.apache.commons.codec.binary.Base64

import scala.util.{Failure, Success, Try}

trait BasicHttpAuthentication {
  private val authTypePrefix = "Basic "

  def decodeBasicAuth(auth: String): Try[(String, String)] = {
    Try(new String(new Base64().decode(auth.drop(authTypePrefix.length)), "UTF-8")) match {
      case Success(userColonPass) =>
        val split = userColonPass.split(":", 2)
        if (split.length != 2)
          Failure(new IllegalArgumentException(s"$auth is not a valid Basic header"))
        else
          Success(split(0)->split(1))
      case Failure(_) => Failure(new IllegalArgumentException(s"$auth is not a valid Basic header"))
    }
  }
}
