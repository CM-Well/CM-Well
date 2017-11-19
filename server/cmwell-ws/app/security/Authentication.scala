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

import authentikat.jwt._
import cmwell.ws.Settings
import com.github.t3hnar.bcrypt._
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import play.api.libs.json.JsValue

/**
 * Created by yaakov on 1/20/15.
 */

object Authentication {

//  private lazy val secret = ConfigFactory.load().getString("play.http.secret.key") // not using ws.Settings, so it'd be available from `sbt ws/console`
//  private val jwtHeader = JwtHeader("HS256")

  def passwordMatches(user: JsValue, password:String): Boolean = {
    password.isBcrypted((user\"digest").asOpt[String].getOrElse(""))
  }

//  def generateToken(username: String, expiry: Option[DateTime] = None, rev: Option[Int] = None, isAdmin: Boolean = false): String = {
//    val maxDays = Settings.maxDaysToAllowGenerateTokenFor
//    if(!isAdmin && expiry.isDefined && expiry.get.isAfter(DateTime.now.plusDays(maxDays)))
//      throw new IllegalArgumentException(s"Token expiry must be less than $maxDays days")
//    if(!isAdmin && rev.isDefined)
//      throw new IllegalArgumentException(s"rev should only be supplied in Admin mode (i.e. manually via console)")
//
//    val claims = Map("sub" -> username, "exp" -> expiry.getOrElse(DateTime.now.plusDays(1)).getMillis, "rev" -> rev.getOrElse(getUserRevNum(username)))
//    JsonWebToken(jwtHeader, JwtClaimsSet(claims), secret)
//  }

//  def validateToken(jwt: String) = {
//    jwt match {
//      case JsonWebToken(_, claimsSet, _) =>
//        JsonWebToken.validate(jwt, secret) &&
//          new DateTime(claimsSet.getClaimValue("exp").map(_.toLong).getOrElse(0L)).isAfterNow &&
//          claimsSet.getClaimValue("rev").map(_.toInt).getOrElse(0) >= claimsSet.getClaimValue("sub").map(getUserRevNum).getOrElse(0)
//      case _ => false
//    }
//  }
//
//  def extractUsernameFromToken(jwt: String): Option[String] = {
//    jwt match {
//      case JsonWebToken(_, claimsSet, _) => claimsSet.getClaimValue("sub")
//      case _ => None
//    }
//  }

//  private def getUserRevNum(username: String) = AuthCache.getUserInfoton(username).flatMap(u => (u\"rev").asOpt[Int]).getOrElse(0)


//  implicit class JwtClaimsSetJValueExtensions(cs: JwtClaimsSetJValue) {
//    def getClaimValue(key: String): Option[String] = cs.asSimpleMap.toOption.getOrElse(Map()).get(key)
//  }
}
