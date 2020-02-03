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
package cmwell.ctrl.ddata

import k.grid.dmap.api.{SettingsSet, SettingsString}
import k.grid.dmap.impl.inmem.InMemDMap

/**
  * Created by michael on 3/29/15.
  */
object DData {
  def getDownNodes: Set[String] = InMemDMap.get("downNodes").asInstanceOf[Option[SettingsSet]] match {
    case Some(s) => s.set
    case None    => Set.empty[String]
  }

  def setDownNodes(s: Set[String]) = {
    if (getDownNodes != s)
      InMemDMap.set("downNodes", SettingsSet(s))
  }

  def addDownNode(n: String) = {
    InMemDMap.aggregate("downNodes", n)
  }

  def getKnownNodes: Set[String] = {
    InMemDMap.get("knownHosts").asInstanceOf[Option[SettingsSet]] match {
      case Some(s) => s.set
      case None    => Set.empty[String]
    }

  }

  def setKnownNodes(s: Set[String]) = {
    if (getKnownNodes != s)
      InMemDMap.set("knownHosts", SettingsSet(s))
  }

  def addKnownNode(n: String) {
    InMemDMap.aggregate("knownHosts", n)
  }

  def setPingIp(pip: String) = {
    InMemDMap.set("pingIp", SettingsString(pip))
  }

  def getPingIp: String = {
    InMemDMap.get("pingIp") match {
      case Some(sVal) => sVal.asInstanceOf[SettingsString].str
      case None       => ""
    }
  }

  def getEsMasters: Set[String] = InMemDMap.get("esMasters").asInstanceOf[Option[SettingsSet]] match {
    case Some(s) => s.set
    case None    => Set.empty[String]
  }

  def setEsMasters(s: Set[String]) = {
    if (getEsMasters != s)
      InMemDMap.set("esMasters", SettingsSet(s))
  }

  def addEsMaster(n: String) {
    InMemDMap.aggregate("esMasters", n)
  }

  def removeEsMaster(n: String) {
    InMemDMap.subtract("esMasters", n)
  }

}
