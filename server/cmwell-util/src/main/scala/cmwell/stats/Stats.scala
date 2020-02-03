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
package cmwell.stats

import com.typesafe.config.ConfigFactory

/**
  * Created with IntelliJ IDEA.
  * User: Michael
  * Date: 4/3/14
  * Time: 7:54 PM
  * To change this template use File | Settings | File Templates.
  */
object Stats {

  object Settings {
    val config = ConfigFactory.load()
    val domainName = config.getString("stats.domainName")
    val host = config.getString("stats.host")
    val port = config.getInt("stats.port")

  }

  var withDate = false

  /**
    * Creates a StatsSender object.
    * @param path formats: some.nice.path | some.machine.related.statistics.{MachineName} | some.date.related.statistics.{DateTime}
    * @return
    */
  def getStatsSender(path: String) = new StatsSender(Settings.domainName + "." + path, Settings.host, Settings.port)

  /**
    * Creates a StopWatch object. Use the method stop in order to send the data.
    * @param path formats: some.nice.path | some.machine.related.statistics.{MachineName} | some.date.related.statistics.{DateTime}
    * @param action the name of the action
    * @return
    */
  def getStopWatch(path: String, action: String): StopWatch = {
    val sSender = new StatsSender(Settings.domainName + "." + path, Settings.host, Settings.port)
    new StopWatch(sSender, action)
  }

  /**
    * Measures the time it takes to execute a block of code.
    * @param path formats: some.nice.path | some.machine.related.statistics.{MachineName} | some.date.related.statistics.{DateTime}
    * @param action the name of the action
    * @return
    */
  def measureBlock(path: String, action: String)(code: => Unit): Unit = {
    val sSender = new StatsSender(Settings.domainName + "." + path, Settings.host, Settings.port)
    val sWatch = new StopWatch(sSender, action)
    code
    sWatch.stop
  }
}
