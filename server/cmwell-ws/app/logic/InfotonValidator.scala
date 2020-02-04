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
package logic
import cmwell.domain._
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by gilad on 6/18/14.
  */
object InfotonValidator extends LazyLogging {
  def isInfotonNameValid(path: String): Boolean = {
    val noSlash = if (path.startsWith("/")) path.dropWhile(_ == '/') else path
    (!(noSlash.matches("_(.)*|(ii|zz|proc)(/(.)*)?"))) match {
      case true  => true
      case false => logger.warn(s"validation failed for infoton path: $path"); false
    }
  }

  type Fields[K] = Map[K, Set[FieldValue]]
  def validateValueSize[K](fields: Fields[K]): Unit =
    if (fields.exists { case (_, s) => s.exists(_.size > cmwell.ws.Settings.maxValueWeight) })
      throw new IllegalArgumentException("uploaded infoton, contains a value heavier than 16K.")
}
