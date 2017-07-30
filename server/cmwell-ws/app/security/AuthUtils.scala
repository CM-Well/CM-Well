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

import javax.inject.Inject

import cmwell.domain.{FieldValue, FileContent, FileInfoton, Infoton}
import cmwell.ws.Settings
import com.github.t3hnar.bcrypt._
import logic.CRUDServiceFS
import play.api.libs.json.{JsObject, JsString}
import play.api.mvc.Request
import security.PermissionLevel.PermissionLevel

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Random

class AuthUtils @Inject()(authCache: AuthCache, authorization: Authorization, crudServiceFS: CRUDServiceFS) {
  def changePassword(token: Token, currentPw: String, newPw: String): Future[Boolean] = {
    if(!token.isValid) {
      Future.successful(false)
    } else {
      authCache.getUserInfoton(token.username) match {
        case Some(user) if Authentication.passwordMatches(user, currentPw) => {
          val digestValue = newPw.bcrypt(generateSalt)
          val digest2Value = cmwell.util.string.Hash.md5(s"${token.username}:cmwell:$newPw")
          val newUserInfoton = user.as[JsObject] ++ JsObject(Seq("digest" -> JsString(digestValue), "digest2" -> JsString(digest2Value)))
          crudServiceFS.putInfoton(FileInfoton(s"/meta/auth/users/${token.username}", Settings.dataCenter, None, Map.empty[String, Set[FieldValue]], FileContent(newUserInfoton.toString.getBytes, "application/json")))
        }
        case _ => Future.successful(false)
      }
    }
  }

  val useAuthorizationParam: Boolean = java.lang.Boolean.getBoolean("use.authorization")

  def extractTokenFrom(req: Request[_]) = {
    val oldKey = "X-CM-WELL-TOKEN"
    val key = "X-CM-WELL-TOKEN2" // todo TOKEN2 is only supported for backward compatibility. one day we should stop supporting it
    val jwtOpt = req.headers.get(key).
      orElse(req.headers.get(oldKey)).
      orElse(req.getQueryString("token")).
      orElse(req.cookies.get(key).map(_.value)).
      orElse(req.cookies.get(oldKey).map(_.value))

    jwtOpt.flatMap(Token(_,authCache))
  }

  def isValidatedAs(tokenOpt: Option[Token], expectedUsername: String) = tokenOpt.exists(token => token.isValid && token.username == expectedUsername)

  //todo find a better name for this method
  def filterNotAllowedPaths(paths: Iterable[String], level: PermissionLevel, tokenOpt: Option[Token]): Iterable[String] = {
    val doesRequestContainWritesToMeta = paths.exists(isWriteToMeta(level, _))
    if (!useAuthorizationParam && !doesRequestContainWritesToMeta)
      return Seq()

    tokenOpt match {
      case Some(token) if token.isValid => {
        authCache.getUserInfoton(token.username) match {
          case Some(user) => paths.filterNot(path => authorization.isAllowedForUser((path, level), user))
          case None if token.username == "root" || token.username == "pUser" => Seq() // special case only required for cases when CRUD is not yet ready
          case None => paths
        }
      }
      case _ => paths.filterNot(authorization.isAllowedForAnonymousUser(_, level))
    }
  }

  def isOperationAllowedForUser(op: Operation, token: Option[Token], evenForNonProdEnv: Boolean = false): Boolean = {
    if(!useAuthorizationParam && !evenForNonProdEnv)
      true
    else
      getUser(token).exists(authorization.isOperationAllowedForUser(op, _))
  }

  // todo rather than boolean result, one can return (deep-)filtered Seq (multitanency)
  def isContentAllowedForUser(infotons: Seq[Infoton], token: Option[Token]) = {
    if(!useAuthorizationParam) {
      true
    } else {
      val fields = infotons.flatMap(_.fields).map(_.keySet).reduceLeft(_ ++ _)
      getUser(token).exists(authorization.areFieldsAllowedForUser(fields, _))
    }
  }

  def generateRandomPassword(length: Int = 10) = {
    import com.github.t3hnar.bcrypt._
    val password = Random.alphanumeric take length mkString
    val bcryptedPassword = password bcrypt generateSalt
    (password, bcryptedPassword)
  }

  def isWriteToMeta(level: PermissionLevel, path: String) = {
    level == PermissionLevel.Write && path.matches("/meta/.*") && !path.matches("/meta/sys/dc/.*")
  }

  private def getUser(tokenOpt: Option[Token]) =
    tokenOpt.collect{ case token if token.isValid => authCache.getUserInfoton(token.username) }.flatten

}
