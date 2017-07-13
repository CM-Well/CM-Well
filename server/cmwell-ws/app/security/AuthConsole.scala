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


package security

object AuthConsole {
  def help = {
    println("""
      |To generate a valid token for a user, invoke the genereateToken method, supply a username as the first argument.
      |
      |Second optional argument is expiry, you can supply a string which will be parsed by DateTime.parse
      |When not supplied, the generated token will be valid for the next 24 hours.
      |
      |Third optional argument is rev, you will need to supply an int only for exiting user which
      |his/her token was ever live-revoked. // If there's a UserInfoton, the rev number should match.
      |When in doubt, curl /meta/auth/users/USERNAME | jq ".rev" # if it's null you can leave it blank.
      |
      |
      |You can test the results on the "Minimal JWT Debugger" (available in UI by clicking the robot's arm)
    """.stripMargin)
  }

  val unusedCache = null.asInstanceOf[AuthCache]

  def generateToken(username: String): String = {
    Token.generate(unusedCache, username, None, Option(0), isAdmin = true)
  }

  def generateToken(username: String, expiry: String, rev: Int = 1): String = {
    Token.generate(unusedCache, username, Some(org.joda.time.DateTime.parse(expiry)), Option(rev), isAdmin = true)
  }
}
