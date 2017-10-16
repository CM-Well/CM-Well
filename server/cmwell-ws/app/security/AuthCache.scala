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

import cmwell.domain.{Everything, FileInfoton}
import cmwell.ws.Settings
import cmwell.zcache.L1Cache
import cmwell.util.concurrent.retryUntil
import cmwell.util.numeric.Radix64.encodeUnsigned
import com.typesafe.scalalogging.LazyLogging
import logic.CRUDServiceFS
import org.joda.time.DateTime
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Success, Failure, Try}

@Singleton
class AuthCache @Inject()(crudServiceFS: CRUDServiceFS)(implicit ec: ExecutionContext) extends LazyLogging {
  private val usersFolder = "/meta/auth/users"
  private val rolesFolder = "/meta/auth/roles"

  def getRole(roleName: String): Option[JsValue] = safelyGetEntity(roleName)(rolesCache)
  def getUserInfoton(userName: String): Option[JsValue] = safelyGetEntity(userName)(usersCache)

  // TODO Do not Await.result... These should return Future[Option[JsValue]]]
  private def safelyGetEntity(entityName: String)(source: String => Future[Option[JsValue]]): Option[JsValue] = {
    val timestamp = DateTime.now()
    val logID = encodeUnsigned(Thread.currentThread().getId) + "~" + encodeUnsigned(timestamp.getMillis)
    Try {
      val entityFut = source(entityName)
      val timestamp = DateTime.now()
      Await.result(entityFut.andThen {
        case Failure(err) => logger.error(s"[$logID] failed to get entity[$entityName]", err)
        case Success(ojv) if DateTime.now().compareTo(timestamp.plus(3000L)) > 0 => logger.error(s"[$logID] succeeded, but took more than 3s. value is: $ojv")
      }, 3.seconds)
    }.transform(Success.apply, err => {
      logger.error(s"[$logID] failed to get entity[$entityName] with thrown exception",err)
      Success(Option.empty[JsValue])
    }).get
  }

  private def getUserFromCas(username: String) = getFromCrudAndExtractJson(s"$usersFolder/$username")
  private def getRoleFromCas(rolename: String) = getFromCrudAndExtractJson(s"$rolesFolder/$rolename")

  private def getFromCrudAndExtractJson(infotonPath: String) = retryUntil[Option[JsValue]](_.isDefined, 3, 100.millis) {
    crudServiceFS.getInfoton(infotonPath, None, None).map {
      case Some(Everything(FileInfoton(_, _, _, _, _, Some(c), _))) =>
        Some(Json.parse(new String(c.data.get, "UTF-8")))
      case other =>
        logger.warn(s"Trying to read $infotonPath but got from CAS $other")
        None
    }
  }

  private val usersCache = L1Cache.memoize(task = getUserFromCas)(
                                           digest = identity,
                                           isCachable = _.isDefined)(
                                           l1Size = Settings.zCacheL1Size,
                                           ttlSeconds = Settings.zCacheSecondsTTL)

  private val rolesCache = L1Cache.memoize(task = getRoleFromCas)(
                                           digest = identity,
                                           isCachable = _.isDefined)(
                                           l1Size = Settings.zCacheL1Size,
                                           ttlSeconds = Settings.zCacheSecondsTTL)
}
