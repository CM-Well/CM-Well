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


package k.grid.dmap.impl.persistent.json

import k.grid.dmap.api._

import spray.json.JsValue
import spray.json._

/**
 * Created by michael on 5/29/16.
 */

case object PersistentDMapDataFileParsingException extends Exception{
  override def getMessage: String = "Couldn't parse persistent data file"
}

object MapDataJsonProtocol {

  implicit object SettingsValue extends JsonFormat[SettingsValue] {
    override def write(obj: SettingsValue): JsValue = {
      obj match {
        case SettingsString(str) => JsString(str)
        case SettingsBoolean(bool) => JsBoolean(bool)
        case SettingsLong(lng) => JsNumber(lng)
        case SettingsSet(set) => JsArray(set.toVector.map(s => JsString(s)))
      }
    }

    override def read(json: JsValue): SettingsValue = json match {
      case JsString(s) => SettingsString(s)
      case JsBoolean(b) => SettingsBoolean(b)
      case JsNumber(n) => SettingsLong(n.toLong)
      case JsArray(arr) => SettingsSet(arr.collect{case JsString(s) => s}.toSet)
      case _ => throw PersistentDMapDataFileParsingException
    }
  }

  implicit object MapJsonFormat extends JsonFormat[Map[String, SettingsValue]] {
    override def write(obj: Map[String, SettingsValue]): JsValue = {
      val j = obj.map {
        tup =>
          tup._1 -> tup._2.toJson
      }
      JsObject(j)
    }

    override def read(json: JsValue): Map[String, SettingsValue] = json match {
      case JsObject(obj) => obj.map {
        o =>
          o._1 -> o._2.convertTo[SettingsValue]
      }
      case _ => throw PersistentDMapDataFileParsingException
    }
  }

  implicit object MapDataJsonFormat extends JsonFormat[MapData] {
    override def write(obj: MapData): JsValue = {
      JsObject("timestamp" -> JsNumber(obj.timestamp), "data" -> obj.m.toJson)
    }

    override def read(json: JsValue): MapData = (json: @unchecked) match {
      case JsObject(obj) => {
        val timestamp = (obj("timestamp"): @unchecked) match {
          case JsNumber(n) => n.toLong
        }

        val data = obj("data").convertTo[Map[String,SettingsValue]]

        MapData(data, timestamp)
      }
    }
  }
}
