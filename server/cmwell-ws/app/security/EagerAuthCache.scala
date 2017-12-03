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

import javax.inject.{Inject, Singleton}

import cmwell.domain.{Everything, FileContent, FileInfoton, Infoton}
import cmwell.fts.{PaginationParams, PathFilter}
import cmwell.util.concurrent._
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

@Singleton
class EagerAuthCache @Inject()(crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext) extends LazyLogging {
  private[this] var oData: AuthData = AuthData.empty
  private[this] var nData: AuthData = AuthData.empty

  //TODO use AtomicBoolean!
  private[this] var isLoadingSemaphore: Boolean = false

  private def data(nbg: Boolean) = if(nbg) nData else oData

  SimpleScheduler.scheduleAtFixedRate(1.minute, 15.minutes)(load(true))
  SimpleScheduler.scheduleAtFixedRate(2.minutes, 15.minutes)(load(false))

  // TODO Do not Await.result... These should return Future[Option[JsValue]]]
  def getRole(roleName: String, nbg: Boolean): Option[JsValue] = {
    data(nbg).roles.get(roleName).orElse {
      Await.result(directReadFallback(s"/meta/auth/roles/$roleName", nbg), 6.seconds).map { role =>
          logger.info(s"AuthCache(nbg=$nbg) role $roleName was not in memory, but added to Map")
          if(nbg)
            nData = nData.copy(roles = nData.roles + (roleName -> role))
          else
            oData = oData.copy(roles = oData.roles + (roleName -> role))
        role
      }
    }
  }

  // TODO Do not Await.result... These should return Future[Option[JsValue]]]
  def getUserInfoton(userName: String, nbg: Boolean): Option[JsValue] = {
    data(nbg).roles.get(userName).orElse {
      Await.result(directReadFallback(s"/meta/auth/users/$userName", nbg), 6.seconds).map { user =>
        logger.debug(s"AuthCache(nbg=$nbg) user $userName was not in memory, but added to Map")
        if(nbg)
          nData = nData.copy(users = nData.users + (userName -> user))
        else
          oData = oData.copy(users = oData.users + (userName -> user))
        user
      }
    }
  }

  def invalidate(nbg: Boolean): Future[Boolean] = if(isLoadingSemaphore) Future.successful(false) else load(nbg)

  // TODO use testAndSet in both cases, manual invalidate as well as scheduled load.
  private def load(nbg: Boolean): Future[Boolean] = {
    isLoadingSemaphore = true

    retryUntil[AuthData](isSuccessful = !_.isEmpty, maxRetries = 10, delay = 5.seconds)(loadOnce(false)).map { data =>
      if(nbg) nData = combiner(nData, data)
      else oData = combiner(oData, data)
      isLoadingSemaphore = false
      true
    }.recover { case _ =>
      isLoadingSemaphore = false
      false
    }
  }

  private def loadOnce(nbg: Boolean): Future[AuthData] = {
    // one level under /meta/auth is a parent (e.g. "users", "roles")
    def isParent(infoton: Infoton) = infoton.path.count(_ == '/') < 4
    logger.info(s"AuthCache(nbg=$nbg) is now loading...")
    crudServiceFS.search(Some(PathFilter("/meta/auth", descendants = true)), withData = true, paginationParams = PaginationParams(0, 2048), nbg = nbg).map { searchResult =>
      val data = searchResult.infotons.filterNot(isParent).map(i => i.path -> extractPayload(i)).collect { case (p, Some(jsv)) => p -> jsv }.toMap
      val (usersData, rolesData) = cmwell.util.collections.partitionWith(data) { t =>
        val (path, payload) = t
        val isUser = path.startsWith("/meta/auth/users")
        val key = path.substring(path.lastIndexOf("/") + 1)
        if (isUser) Left(key -> payload)
        else Right(key -> payload)
      }
      logger.info(s"AuthCache(nbg=$nbg) Loaded with ${usersData.size} users and ${rolesData.size} roles.")
      AuthData(usersData.toMap, rolesData.toMap)
    }.recover { case t: Throwable =>
      logger.error(s"AuthCache(nbg=$nbg) failed to load", t)
      AuthData.empty
    }
  }

  private def directReadFallback(infotonPath: String, nbg: Boolean) = crudServiceFS.getInfoton(infotonPath, None, None, nbg = nbg).map {
    case Some(Everything(i)) =>
      extractPayload(i)
    case other =>
      logger.warn(s"AuthCache(nbg=$nbg) Trying to read $infotonPath but got from CAS $other")
      None
  }

  private def extractPayload(infoton: Infoton): Option[JsValue] = infoton match {
    case FileInfoton(_, _, _, _, _, Some(FileContent(Some(payload), _, _, _)), _) =>
      val jsValOpt = Try(Json.parse(payload)).toOption
      if(jsValOpt.isEmpty)
        logger.warn(s"AuthCache Infoton(${infoton.path}) has invalid JSON content.")
      jsValOpt
    case _ =>
      logger.warn(s"AuthCache Infoton(${infoton.path}) does not exist, or is not a FileInfoton with valid content.")
      None
  }

  private def combiner(ad1: AuthData, ad2: AuthData): AuthData = if(ad2.isEmpty) ad1 else ad2

  // todo - Leave JsValue for parsing only. Use case classes for User and Role !
  case class AuthData(users: Map[String, JsValue], roles: Map[String, JsValue]) {
    def isEmpty: Boolean = users.isEmpty && roles.isEmpty
  }
  object AuthData {
    val empty = AuthData(Map.empty, Map.empty)
  }
}
