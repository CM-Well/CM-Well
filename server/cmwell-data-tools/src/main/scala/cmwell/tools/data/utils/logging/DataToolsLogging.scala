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
package cmwell.tools.data.utils.logging

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory

case class LabelId(id: String)

trait DataToolsLogging {
  private[data] lazy val redLogger = Logger(LoggerFactory.getLogger("tools-red-logger"))
  private[data] lazy val badDataLogger = Logger(LoggerFactory.getLogger("tools-bad-data"))

  val label: Option[String] = None

  protected lazy val logger: Logger = {
    val loggerName = if (label.isEmpty) getClass.getName else s"${getClass.getName} [${label.get}]"
    Logger(LoggerFactory.getLogger(loggerName))
  }
}
