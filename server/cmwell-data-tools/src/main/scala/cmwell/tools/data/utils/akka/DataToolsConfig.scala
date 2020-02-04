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
package cmwell.tools.data.utils.akka

import com.typesafe.config.ConfigFactory

/**
  * Contains Typesafe Config which is loaded and configured once to
  * all data-tools modules
  */
trait DataToolsConfig {
  private[data] lazy val config = {
    val dataToolsPath = "data-tools"
    val defaultConfig = ConfigFactory.load()

    //only the data-tools section but it's treated and the root config
    val dataToolsSection = defaultConfig.getConfig(dataToolsPath)

    //the data-tools as root with fallback to default config of the real root
    dataToolsSection.withFallback(defaultConfig.withoutPath(dataToolsPath))
  }
}
