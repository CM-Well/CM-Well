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

import cmwell.domain.{Everything, FileInfoton, Infoton}
import cmwell.fts.PathFilter
import cmwell.util.concurrent.{Combiner, SingleElementLazyAsyncCache, Validator}
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@Singleton
class AuthCache @Inject()(crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext) extends LazyLogging {
  // TODO Do not Await.result... These should return Future[Option[JsValue]]]
  def getRole(roleName: String): Option[JsValue] = Await.result(data.getAndUpdateIfNeeded.map(_.roles.get(roleName)).recoverWith { case _ =>
    logger.warn(s"AuthCache Graceful Degradation: Search failed! Trying direct read for Role($roleName):")
    getFromCrudAndExtractJson(s"/meta/auth/roles/$roleName")
  }, 5.seconds)

  def getUserInfoton(userName: String): Option[JsValue] = Await.result(data.getAndUpdateIfNeeded.map(_.users.get(userName)).recoverWith { case _ =>
    logger.warn(s"AuthCache Graceful Degradation: Search failed! Trying direct read for User($userName):")
    getFromCrudAndExtractJson(s"/meta/auth/users/$userName")
  }, 5.seconds)

  def invalidate(): Boolean = data.reset().isSuccess

  private def getFromCrudAndExtractJson(infotonPath: String) = crudServiceFS.getInfoton(infotonPath, None, None).map {
    case Some(Everything(i)) =>
      extractPayload(i)
    case other =>
      logger.warn(s"Trying to read $infotonPath but got from CAS $other")
      None
  }

  private implicit val authDataValidator: Validator[AuthData] = new Validator[AuthData] {
    override def isValid(authData: AuthData) = !authData.isEmpty
  }
  private val data = new SingleElementLazyAsyncCache[AuthData](5 * 60000, initial = AuthData.empty)(load())

  private def load(): Future[AuthData] = {
    logger.info("AuthCache Loading...")

    // one level under /meta/auth is a parent (e.g. "users", "roles")
    def isParent(infoton: Infoton) = infoton.path.count(_ == '/') < 4

    crudServiceFS.search(Some(PathFilter("/meta/auth", descendants = true)), withData = true).map { searchResult =>
      val data = searchResult.infotons.filterNot(isParent).map(i => i.path -> extractPayload(i)).collect { case (p, Some(jsv)) => p -> jsv }.toMap
      val (usersData, rolesData) = cmwell.util.collections.partitionWith(data) { t =>
        val (path, payload) = t
        val isUser = path.startsWith("/meta/auth/users")
        val key = path.substring(path.lastIndexOf("/") + 1)
        if (isUser) Left(key -> payload)
        else Right(key -> payload)
      }
      logger.info(s"AuthCache Loaded with ${usersData.size} users and ${rolesData.size} roles.")
      AuthData(usersData.toMap, rolesData.toMap)
    }.recover { case t: Throwable =>
      logger.info(s"AuthCache failed to load because ${t.getMessage}")
      AuthData.empty
    }
  }

  private def extractPayload(infoton: Infoton): Option[JsValue] = infoton match {
    case FileInfoton(_, _, _, _, _, Some(c), _) =>
      Some(Json.parse(new String(c.data.get, "UTF-8")))
    case _ =>
      logger.warn(s"AuthInfoton(${infoton.path}) does not exist, or is not a FileInfoton with valid JSON content.")
      None
  }

  case class AuthData(users: Map[String, JsValue], roles: Map[String, JsValue]) {
    def isEmpty: Boolean = users.isEmpty && roles.isEmpty
  }
  object AuthData {
    val empty = AuthData(Map.empty, Map.empty)
  }
}