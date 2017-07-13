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

import javax.inject._

import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json._
import security.PermissionLevel.PermissionLevel

object Authorization extends LazyLogging {
  // todo we only need those for cases when CRUD is not yet initialized (for example during install). Can we have an onInitialized callback?
  private val defaultAnonymousUser = Json.parse("""{"paths":[{"id":"/","recursive":true,"sign":"+","permissions":"r"},{"id":"/meta/ns","recursive":true,"sign":"-","permissions":"rw"},{"id":"/meta/auth","recursive":true,"sign":"-","permissions":"rw"}],"roles":[]}""")

  implicit class StringExtensions(s: String) {
    def appendSlash = if(s.endsWith("/")) s else s+"/"

    // cannot use String.startsWith straightforward, because "/foobar/bar" is not subfolder of "/foo"
    def isSubfolderOf(path: String) = {
      val normalizedPath = path.appendSlash
      s.appendSlash==normalizedPath || (path.length < s.length && s.startsWith(normalizedPath))
    }

    // cannot use ==  straightforward, because "/foo/" is same as "/foo"
    def isSameAs(path: String) = s.appendSlash == path.appendSlash
  }

  implicit class JsValueExtensions(v: JsValue) {
    def getArr(prop: String): Seq[JsValue] = (v \ prop).getOrElse(JsArray(Seq())) match { case JsArray(seq) => seq case _ => Seq() }
  }
}

@Singleton
class Authorization @Inject()(authCache: AuthCache) extends LazyLogging {
  import Authorization._

  private def anonymousUser = authCache.getUserInfoton("anonymous").getOrElse(defaultAnonymousUser)

  def isAllowedForAnonymousUser(request: (String, PermissionLevel)): Boolean = {
    isAllowedForUser(request, anonymousUser)
  }

  // todo - is it a good idea to have the username as a prop within UserInfoton Json?, if so, we won't need that extra username parameter
  def isAllowedForUser(request: (String, PermissionLevel), user: JsValue, username: Option[String] = None): Boolean = {
    def relevant(path: JsValue): Boolean = {
      val levels = (path \ "permissions").as[String].map{ case 'r' => PermissionLevel.Read; case 'w' => PermissionLevel.Write }.toSet

      if(!levels(request._2))
        return false

      val fullPath = (path \ "id").as[String]
      val rec = (path \ "recursive").asOpt[Boolean].getOrElse(false)
      request._1.isSameAs(fullPath) || rec && request._1.isSubfolderOf(fullPath)
    }

    // user is allowed to view his/her UserInfoton:
    if(request._1 == s"/meta/auth/users/${username.getOrElse("")}" && request._2 == PermissionLevel.Read)
      return true

    def isPositive(path: JsValue) = (path \ "sign").as[String] == "+"

    val specificPaths = user getArr "paths" filter(relevant)
    val (allow, deny) = specificPaths.partition(isPositive)

    if(specificPaths.nonEmpty)
      return allow.nonEmpty && deny.isEmpty

    def getRolesPaths(roleName: JsValue) = {
      authCache.getRole(roleName.as[String]) match {
        case Some(role) => role getArr "paths"
        case None => logger error s"Role $roleName was not found"; Seq()
      }
    }

    val rolesPaths = (user getArr "roles").flatMap(getRolesPaths).filter(relevant)
    val (allowByRole, denyByRole) = rolesPaths.partition(isPositive)

    allowByRole.nonEmpty && denyByRole.isEmpty
  }

  def isOperationAllowedForUser(op: Operation, user: JsValue): Boolean = {
    def extractOperationsFrom(userOrRole: JsValue) = (userOrRole getArr "operations").map(_.as[String]).collect{ case Operation(op) => op }.toSet

    val specifiecOps = extractOperationsFrom(user)


    specifiecOps(op) || inRoles(user)(extractOperationsFrom(_)(op))
  }

  def areFieldsAllowedForUser(fields: Set[String], user: JsValue): Boolean = {
    // todo design fields permissions mechanism: there should be "*" for "all fields", there should be "allow" and "deny".
    // todo   anonymous user should have `{ "allow": "*" }`
    // todo   there shouldn't be any restrictions on fields in terms of write. in only masks output
    ???
  }

  def inRoles(user: JsValue)(extractor: JsValue => Boolean) = {
    val roles = (user getArr "roles").flatMap(r => authCache.getRole(r.as[String]))
    roles.exists(extractor)
  }
}
