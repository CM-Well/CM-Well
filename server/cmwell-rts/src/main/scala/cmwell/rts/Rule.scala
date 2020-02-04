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
package cmwell.rts

import scala.util.hashing.MurmurHash3
import cmwell.domain.{FieldValue, Infoton}
import cmwell.formats.FormatType

/**
  * Created by markz on 7/10/14.
  */
// message

sealed abstract class TransmitType
case class Push(handler: (Seq[String]) => Unit) extends TransmitType
case class Pull(format: FormatType) extends TransmitType

class Path(path: String, val recursive: Boolean) extends Serializable {
  val hashValue: Int = MurmurHash3.stringHash(path)
  def length = path.length
  def hash: Int = hashValue

  override def toString() = s"path [$path] recursive [$recursive]"

  def check(p: String): Boolean = {
    if (length > p.length) {
      // the path is too short
      false
    } else
      path == p || {
        val prefixPath = p.take(length)
        if (hashValue == MurmurHash3.stringHash(prefixPath)) {
          // the prefix path is match now need to check also if it is recursive
          recursive || !p.drop(length + 1).contains('/')
        } else {
          false
        }
      }
  }
}

class MatchMap(fields: Map[String, Set[FieldValue]]) extends Serializable {

  // here we get the keys list from the tested infoton
  def check(f: Map[String, Set[FieldValue]]): Boolean = {
    if (fields.isEmpty)
      true
    else {
      val it = fields.iterator
      // if found than stop = true
      var stop: Boolean = false
      while (!stop && it.hasNext) {
        val (item, s) = it.next()
        f.get(item) match {
          case Some(fs) =>
            if (s.isEmpty)
              stop = true
            else {
              val i = s.intersect(fs)
              if (!i.isEmpty) {
                stop = true
              }
            }
          case None =>
        }

      }
      stop
    }
  }
}

sealed abstract class Rule
case object NoFilter extends Rule
case class PathFilter(path: Path) extends Rule
case class MatchFilter(matchMap: MatchMap) extends Rule
case class PMFilter(path: Path, matchMap: MatchMap) extends Rule

object Rule {
  def apply() = NoFilter
  def apply(path: String, recursive: Boolean) = PathFilter(new Path(path, recursive))
  def apply(fields: Map[String, Set[FieldValue]]) = MatchFilter(new MatchMap(fields))
  def apply(path: String, recursive: Boolean, fields: Map[String, Set[FieldValue]]) =
    PMFilter(new Path(path, recursive), new MatchMap(fields))
}
