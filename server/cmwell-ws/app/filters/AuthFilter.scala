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
package filters

import akka.stream.Materializer
import play.api.libs.json.Json
import play.api.mvc.{Filter, RequestHeader, Result, Results}
import security.PermissionLevel._
import security._
import wsutil._
import javax.inject._
import cmwell.ws.Settings

import scala.concurrent.{ExecutionContext, Future}

class AuthFilter @Inject()(authCache: EagerAuthCache, authUtils: AuthUtils, authorization: Authorization)(
  implicit override val mat: Materializer,
  ec: ExecutionContext
) extends Filter {

  private val useAuthorizationParam = java.lang.Boolean.getBoolean("use.authorization")
  private val irrelevantPaths = Set("/ii/", "/_")

  def apply(nextFilter: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] = {
    isAuthenticatedAndAuthorized(requestHeader) match {
      case Allowed(username) =>
        nextFilter(requestHeader.addAttr(Attrs.UserName, username))
      case NotAllowed(msg) if PermissionLevel(requestHeader.method) == PermissionLevel.Read =>
        Future(Results.Forbidden(msg))
      case NotAllowed(msg) =>
        Future(Results.Forbidden(Json.obj("success" -> false, "message" -> msg)))
    }
  }

  private def isAuthenticatedAndAuthorized(requestHeader: RequestHeader): AuthResponse = {
    def isRequestWriteToMeta = authUtils.isWriteToMeta(PermissionLevel(requestHeader.method), requestHeader.path)

    val tokenOpt = requestHeader.headers
      .get("X-CM-WELL-TOKEN2")
      . // todo TOKEN2 is only supported for backward compatibility. one day we should stop supporting it
      orElse(requestHeader.headers.get("X-CM-WELL-TOKEN"))
      .orElse(requestHeader.getQueryString("token"))
      .orElse(requestHeader.cookies.get("X-CM-WELL-TOKEN2").map(_.value))
      . // todo TOKEN2 is only supported for backward compatibility. one day we should stop supporting it
      orElse(requestHeader.cookies.get("X-CM-WELL-TOKEN").map(_.value))
      .flatMap(Token(_, authCache))

    val modifier = tokenOpt.fold("anonymous")(_.username) + requestHeader.getQueryString("modifier").fold("")("/".+)

    if ((!useAuthorizationParam && !isRequestWriteToMeta) || irrelevantPaths.exists(requestHeader.path.startsWith))
      Allowed(modifier)
    else {
      val request =
        (normalizePath(requestHeader.path), PermissionLevel(requestHeader.method, requestHeader.getQueryString("op")))

      tokenOpt match {
        case Some(token) if token.isValid => {
          authCache.getUserInfoton(token.username) match {
            case Some(user) =>
              AuthResponse(authorization.isAllowedForUser(request, user, Some(token.username)),
                "Authenticated but not authorized", modifier)
            case None if token.username == "root" || token.username == "pUser" =>
              Allowed(modifier) // special case only required for cases when CRUD is not yet ready
            case None => NotAllowed(s"Username ${token.username} was not found in CM-Well")
          }
        }
        case Some(_) => NotAllowed("given token is not valid (not signed or expired)")
        case None => AuthResponse(authorization.isAllowedForAnonymousUser(request), "Not authorized, please login first", modifier)
      }
    }
  }

  sealed trait AuthResponse
  case class Allowed(modifier: String) extends AuthResponse
  case class NotAllowed(msg: String) extends AuthResponse

  object AuthResponse {
    def apply(allowed: Boolean, msg: String, modifier: String): AuthResponse =
      if(allowed) Allowed(modifier) else NotAllowed(msg)
  }
}
