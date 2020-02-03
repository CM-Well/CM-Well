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

/**
  * Created with IntelliJ IDEA.
  * User: Michael
  * Date: 4/3/14
  * Time: 7:36 PM
  * To change this template use File | Settings | File Templates.
  */
case class StopWatch(sSender: StatsSender, action: String) {
  private def getTimestamp: Long = {
    System.currentTimeMillis
  }

  val start = getTimestamp

  def stop {
    val diff = getTimestamp - start
    sSender.sendTimings(action, diff.toInt)
  }

}
