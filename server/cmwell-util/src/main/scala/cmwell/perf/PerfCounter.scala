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
package cmwell.perf

import com.ecyrd.speed4j.log.PeriodicalLog
import com.ecyrd.speed4j.{StopWatch, StopWatchFactory}

/**
  * Created with IntelliJ IDEA.
  * User: markz
  * Date: 2/13/13
  * Time: 8:11 AM
  * A little wrapper over speed4j
  */
class PerfCounter(name: String, actions: String, periodSec: Int = 1) {
  // init PeriodicalLog
  val pLog: PeriodicalLog = new PeriodicalLog()
  pLog.setName(name)
  pLog.setPeriod(periodSec)
  pLog.setJmx(actions)
  pLog.setSlf4jLogname(this.getClass.toString + "." + name)
  pLog.setMode(PeriodicalLog.Mode.JMX_ONLY)

  // create a stopwatch factory
  val swf: StopWatchFactory = StopWatchFactory.getInstance(pLog)
  //def getStopWatch(count : Int = 1) : StopWatch = swf.getStopWatch(count)
  def getStopWatch: StopWatch = swf.getStopWatch()
  def shutdown() { pLog.shutdown() }
}
