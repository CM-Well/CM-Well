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

import cmwell.domain.{Everything, FileContent, FileInfoton, Infoton}
import cmwell.fts.{PaginationParams, PathFilter}
import cmwell.util.concurrent.{SingleElementLazyAsyncCache, Validator}
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

@Singleton
class AuthCache @Inject()(crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext) extends LazyLogging {
  // TODO Do not Await.result... These should return Future[Option[JsValue]]]
  def getRole(roleName: String, nbg: Boolean): Option[JsValue] = {
    val f = getAuthInfoton(_.roles.get(roleName), s"/meta/auth/roles/$roleName", nbg)
    Await.result(f, 6.seconds)
  }

  def getUserInfoton(userName: String, nbg: Boolean): Option[JsValue] = {
    val f = getAuthInfoton(_.users.get(userName), s"/meta/auth/users/$userName", nbg)
    Await.result(f, 6.seconds)
  }

  def invalidate(nbg: Boolean): Boolean = data(nbg).reset().isSuccess

  private def getAuthInfoton(picker: AuthData => Option[JsValue], infotonPath: String, nbg: Boolean): Future[Option[JsValue]] = {
    val esDependingFut = data(nbg).getAndUpdateIfNeeded map picker
    // TODO Once FTS supports timeout param, there won't be any need to invoke timeoutFuture.
    cmwell.util.concurrent.timeoutOptionFuture(esDependingFut, 3.seconds).flatMap {
      case Some(authInfotonOpt) =>
        Future.successful(authInfotonOpt)
      case None =>
        logger.warn(s"AuthCache Graceful Degradation: Search failed! Trying direct read for $infotonPath...")
        getFromCasAndExtractJson(infotonPath, nbg)
    }
  }

  private def getFromCasAndExtractJson(infotonPath: String, nbg: Boolean) = crudServiceFS.getInfoton(infotonPath, None, None, nbg = nbg).map {
    case Some(Everything(i)) =>
      extractPayload(i)
    case other =>
      logger.warn(s"Trying to read $infotonPath but got from CAS $other")
      None
  }

  private implicit val authDataValidator: Validator[AuthData] = new Validator[AuthData] {
    override def isValid(authData: AuthData) = !authData.isEmpty
  }


  private def data(nbg: Boolean) = if(nbg) nData else oData
  private val nData = new SingleElementLazyAsyncCache[AuthData](5 * 60000, initial = AuthData.empty)(load(nbg = true))
  private val oData = new SingleElementLazyAsyncCache[AuthData](5 * 60000, initial = AuthData.empty)(load(nbg = false))

  private def load(nbg: Boolean): Future[AuthData] = {

    // one level under /meta/auth is a parent (e.g. "users", "roles")
    def isParent(infoton: Infoton) = infoton.path.count(_ == '/') < 4

    crudServiceFS.search(Some(PathFilter("/meta/auth", descendants = true)), withData = true, paginationParams = PaginationParams(0, 2048), nbg = nbg).map { searchResult =>
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
    case FileInfoton(_, _, _, _, _, Some(FileContent(Some(payload), _, _, _)), _) =>
      val jsValOpt = Try(Json.parse(payload)).toOption
      if(jsValOpt.isEmpty)
        logger.warn(s"AuthInfoton(${infoton.path}) has invalid JSON content.")
      jsValOpt
    case _ =>
      logger.warn(s"AuthInfoton(${infoton.path}) does not exist, or is not a FileInfoton with valid content.")
      None
  }

  // todo - Leave JsValue for parsing only. Use case classes for User and Role !
  case class AuthData(users: Map[String, JsValue], roles: Map[String, JsValue]) {
    def isEmpty: Boolean = users.isEmpty && roles.isEmpty
  }
  object AuthData {
    val empty = AuthData(Map.empty, Map.empty)
  }
}
