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
package actions

import cmwell.domain._
import cmwell.ws.Settings
import wsutil.StringExtensions

/**
  * Created by gilad on 6/22/14.
  */
object ActiveInfotonHandler {

//  private[this] val activeInfotons = Set[String]("/proc","/proc/node")

  def wrapInfotonReply(infoton: Option[Infoton]): Option[Infoton] = infoton match {
    case Some(i) if requiresWrapping(i.path) => Some(wrap(i))
    case i                                   => i
  }

  private[this] def requiresWrapping(path: String): Boolean = path match {
    case "/"                                         => true
    case p if p.startsWith("/proc/")                 => true
    case p if p.dropTrailingChars('/') == "/meta/ns" => true
    case _                                           => false
  }

  import scala.language.implicitConversions
  import VirtualInfoton._

  // todo why do we have to invoke v2i explicitly if it's an implicit def ?!
  private[this] def wrap(infoton: Infoton): Infoton = infoton match {
    case cp @ CompoundInfoton("/", _, _, _, _, children, _, length, total, _) =>
      cp.copy(children = v2i(VirtualInfoton(ObjectInfoton("/proc", Settings.dataCenter))) +: children,
              total = total + 1,
              length = length + 1)
    case cp @ CompoundInfoton("/meta/ns", _, _, _, _, children, _, length, total, _) =>
      cp.copy(
        children = v2i(VirtualInfoton(ObjectInfoton("/meta/ns/sys", Settings.dataCenter))) +: v2i(
          VirtualInfoton(ObjectInfoton("/meta/ns/nn", Settings.dataCenter))
        ) +: children,
        total = total + 2,
        length = length + 2
      )
    case i => i
  }

//  def isActiveInfotonPath(path: String): Boolean = activeInfotons.contains(path)
}
