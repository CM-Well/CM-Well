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
package cmwell.dc

import com.typesafe.scalalogging.{LazyLogging => LazyLoggingWithoutRed}
import org.slf4j.LoggerFactory

trait LazyLogging extends LazyLoggingWithoutRed {
  protected lazy val redlog = LoggerFactory.getLogger("dc_red_log")
  protected lazy val yellowlog = LoggerFactory.getLogger("dc_yellow_log")
}
