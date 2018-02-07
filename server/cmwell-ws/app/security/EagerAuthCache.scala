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
  private[this] var data: AuthData = AuthData.empty

  //TODO use AtomicBoolean!
  private[this] var isLoadingSemaphore: Boolean = false

  // following is ugly. should be called from Global.onStart
  // willing to live with it so to not get many stacktraces on server startup
  private[this] var serverIsWarmingUp = true
  def sometimeAfterStart: Unit = {
    serverIsWarmingUp = false
  }

  SimpleScheduler.scheduleAtFixedRate(2.minutes, 15.minutes)(load())

  // TODO Do not Await.result... These should return Future[Option[JsValue]]]
  def getRole(roleName: String): Option[JsValue] = {
    data.roles.get(roleName).orElse {
      Await.result(directReadFallback(s"/meta/auth/roles/$roleName"), 6.seconds).map { role =>
          logger.debug(s"AuthCache role $roleName was not in memory, but added to Map")
          data = data.copy(roles = data.roles + (roleName -> role))
        role
      }
    }
  }

  // TODO Do not Await.result... These should return Future[Option[JsValue]]]
  def getUserInfoton(userName: String): Option[JsValue] = {
    data.users.get(userName).orElse {
      Await.result(directReadFallback(s"/meta/auth/users/$userName"), 6.seconds).map { user =>
        logger.debug(s"AuthCache user $userName was not in memory, but added to Map")
        data = data.copy(users = data.users + (userName -> user))
        user
      }
    }
  }

  def invalidate(): Future[Boolean] = if(isLoadingSemaphore) Future.successful(false) else load()

  // TODO use testAndSet in both cases, manual invalidate as well as scheduled load.
  private def load(): Future[Boolean] = {
    isLoadingSemaphore = true

    unsafeRetryUntil[AuthData](isSuccessful = !_.isEmpty, maxRetries = 10, delay = 5.seconds)(loadOnce()).map { d =>
      data = combiner(data, d)
      isLoadingSemaphore = false
      true
    }.recover { case _ =>
      isLoadingSemaphore = false
      false
    }
  }

  private def loadOnce(): Future[AuthData] = {
    // one level under /meta/auth is a parent (e.g. "users", "roles")
    def isParent(infoton: Infoton) = infoton.path.count(_ == '/') < 4
    logger.debug(s"AuthCache is now loading...")
    crudServiceFS.search(Some(PathFilter("/meta/auth", descendants = true)), withData = true, paginationParams = PaginationParams(0, 2048)).map { searchResult =>
      val data = searchResult.infotons.filterNot(isParent).map(i => i.path -> extractPayload(i)).collect { case (p, Some(jsv)) => p -> jsv }.toMap
      val (usersData, rolesData) = cmwell.util.collections.partitionWith(data) { t =>
        val (path, payload) = t
        val isUser = path.startsWith("/meta/auth/users")
        val key = path.substring(path.lastIndexOf("/") + 1)
        if (isUser) Left(key -> payload)
        else Right(key -> payload)
      }
      logger.debug(s"AuthCache Loaded with ${usersData.size} users and ${rolesData.size} roles.")
      AuthData(usersData.toMap, rolesData.toMap)
    }.recover { case t: Throwable =>
      logger.error(s"AuthCache failed to load", t)
      AuthData.empty
    }
  }

  private def directReadFallback(infotonPath: String) = crudServiceFS.getInfoton(infotonPath, None, None).map {
    case Some(Everything(i)) =>
      extractPayload(i)
    case other =>
      if(!serverIsWarmingUp) {
        logger.warn(s"AuthCache Trying to read $infotonPath but got from CAS $other")
      }
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
