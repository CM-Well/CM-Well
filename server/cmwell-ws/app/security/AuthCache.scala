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

import java.util.concurrent.TimeUnit

import cmwell.domain.{Everything, FileInfoton}
import com.google.common.cache.{CacheLoader, CacheBuilder, LoadingCache}
import logic.CRUDServiceFS
import play.api.libs.json.{Json, JsValue}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by yaakov on 1/20/15.
 */
object AuthCache {
  private val usersFolder = "/meta/auth/users"
  private val rolesFolder = "/meta/auth/roles"

  def getUserInfoton(username: String): Option[JsValue] =
    Try(userInfotonsCache.get(username)).toOption

  def invalidateUserInfoton(username: String) = userInfotonsCache.invalidate(username)

  def getRole(roleName: String): Option[JsValue] = {
    Try(rolesCache.get(roleName)) match {
      case Success(value) => Some(value)
      case _ => None
    }
  }

  private def getFromCrudAndExtractJson(infotonPath: String) = Await.result(CRUDServiceFS.getInfoton(infotonPath, None, None).map(x => (x: @unchecked) match {
    case Some(Everything(FileInfoton(_,_,_,_,_,Some(c),_))) => Json.parse(new String(c.data.get, "UTF-8"))
  }), Duration.Inf)

  private val userInfotonsCache: LoadingCache[String, JsValue] =
    CacheBuilder
      .newBuilder()
      .maximumSize(128)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(new CacheLoader[String, JsValue] {
        override def load(key: String) = getFromCrudAndExtractJson(s"$usersFolder/$key")
      })

  private val rolesCache: LoadingCache[String, JsValue] =
    CacheBuilder
      .newBuilder()
      .maximumSize(32)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(new CacheLoader[String, JsValue] {
        override def load(key: String) = getFromCrudAndExtractJson(s"$rolesFolder/$key")
      })
}
