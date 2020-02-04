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
package k.grid.dmap.impl.persistent.json

import k.grid.dmap.api._
import play.api.libs.json._

case object PersistentDMapDataFileParsingException extends Exception {
  override def getMessage: String = "Couldn't parse persistent data file"
}

object MapDataJsonProtocol {

  implicit object SettingsValueF extends Format[SettingsValue] {
    override def writes(obj: SettingsValue): JsValue = {
      obj match {
        case SettingsString(str)   => JsString(str)
        case SettingsBoolean(bool) => JsBoolean(bool)
        case SettingsLong(lng)     => JsNumber(lng)
        case SettingsSet(set)      => JsArray(set.toVector.map(s => JsString(s)))
      }
    }

    override def reads(json: JsValue): JsResult[SettingsValue] = json match {
      case JsString(s)  => JsSuccess(SettingsString(s))
      case JsBoolean(b) => JsSuccess(SettingsBoolean(b))
      case JsNumber(n)  => JsSuccess(SettingsLong(n.toLong))
      case JsArray(arr) => JsSuccess(SettingsSet(arr.collect { case JsString(s) => s }.toSet))
      case _            => JsError(PersistentDMapDataFileParsingException.getMessage)
    }
  }

  implicit object MapJsonFormat extends Format[Map[String, SettingsValue]] {
    override def writes(obj: Map[String, SettingsValue]): JsValue = {
      val j = obj.map { tup =>
        tup._1 -> SettingsValueF.writes(tup._2)
      }
      JsObject(j)
    }

    override def reads(json: JsValue): JsResult[Map[String, SettingsValue]] = json match {
      case JsObject(obj) =>
        obj
          .mapValues(SettingsValueF.reads)
          .foldLeft[JsResult[Map[String, SettingsValue]]](JsSuccess(Map.empty[String, SettingsValue])) {
            case (JsSuccess(m, _), (s, JsSuccess(v, _))) => JsSuccess(m + (s -> v))
            case (e: JsError, _)                         => e
            case (_, (_, e: JsError))                    => e
          }
      case _ => JsError(PersistentDMapDataFileParsingException.getMessage)
    }
  }

  implicit object MapDataJsonFormat extends Format[MapData] {
    override def writes(obj: MapData): JsValue = {
      Json.obj("timestamp" -> JsNumber(obj.timestamp), "data" -> MapJsonFormat.writes(obj.m))
    }

    override def reads(json: JsValue): JsResult[MapData] = (json: @unchecked) match {
      case JsObject(obj) => {
        val timestamp = (obj("timestamp"): @unchecked) match {
          case JsNumber(n) => n.toLong
        }

        val data = obj("data").as[Map[String, SettingsValue]]

        JsSuccess(MapData(data, timestamp))
      }
    }
  }
}
