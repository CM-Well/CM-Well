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


package filters

import akka.stream.Materializer
import play.api.libs.json.Json
import play.api.mvc.{Filter, RequestHeader, Result, Results}
import security.PermissionLevel._
import security._
import wsutil._
import javax.inject._

import cmwell.ws.Settings

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

class AuthFilter @Inject()(authCache: AuthCache, authUtils: AuthUtils, authorization: Authorization)(implicit override val mat: Materializer, ec: ExecutionContext) extends Filter {

  private val useAuthorizationParam = java.lang.Boolean.getBoolean("use.authorization")
  private val irrelevantPaths = Set("/ii/", "/_")

  def apply(nextFilter: (RequestHeader) => Future[Result])(requestHeader: RequestHeader): Future[Result] = {
    Settings.authSystemVersion match {
      case 2 =>
        val (allowed, msg) = isAuthenticatedAndAuthorized(requestHeader)

        if (allowed) {
          nextFilter(requestHeader)
        } else {
          if (PermissionLevel(requestHeader.method) == PermissionLevel.Read)
            Future(Results.Forbidden(msg))
          else
            Future(Results.Forbidden(Json.obj("success" -> false, "message" -> msg)))
        }
      case _ => nextFilter(requestHeader)
    }
  }

  private def isAuthenticatedAndAuthorized(requestHeader: RequestHeader): (Boolean, String) = {
    def isRequestWriteToMeta = authUtils.isWriteToMeta(PermissionLevel(requestHeader.method), requestHeader.path)

    if((!useAuthorizationParam && !isRequestWriteToMeta) || irrelevantPaths.exists(requestHeader.path.startsWith))
      return (true, "")

    def withMsg(allowed: Boolean, msg: String) = (allowed, if(allowed) "" else msg)

    val request = (normalizePath(requestHeader.path), PermissionLevel(requestHeader.method, requestHeader.getQueryString("op")))

    val tokenOpt = requestHeader.headers.get("X-CM-WELL-TOKEN2"). // todo TOKEN2 is only supported for backward compatibility. one day we should stop supporting it
      orElse(requestHeader.headers.get("X-CM-WELL-TOKEN")).
      orElse(requestHeader.getQueryString("token")).
      orElse(requestHeader.cookies.get("X-CM-WELL-TOKEN2").map(_.value)). // todo TOKEN2 is only supported for backward compatibility. one day we should stop supporting it
      orElse(requestHeader.cookies.get("X-CM-WELL-TOKEN").map(_.value)).
      flatMap(Token(_,authCache))

    tokenOpt match {
      case Some(token) if token.isValid => {
        authCache.getUserInfoton(token.username) match {
          case Some(user) => withMsg(authorization.isAllowedForUser(request, user, Some(token.username)), "Authenticated but not authorized")
          case None if token.username == "root" || token.username == "pUser" => (true, "") // special case only required for cases when CRUD is not yet ready
          case None => (false, s"Username ${token.username} was not found in CM-Well")
        }
      }
      case Some(_) => (false, "given token is not valid (not signed or expired)")
      case None => withMsg(authorization.isAllowedForAnonymousUser(request), "Not authorized, please login first")
    }
  }
}
