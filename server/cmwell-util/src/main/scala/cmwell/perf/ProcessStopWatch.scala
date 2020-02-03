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
package cmwell.perf

import scala.collection.mutable
import java.text.NumberFormat

class TaskInfo(taskName: String, var timeMillis: Long, var count: Long = 1) {
  def getTaskName: String = taskName
  def getTimeMillis: Long = timeMillis
  def addTimeMillis(addTimeMillis: Long) {
    timeMillis = timeMillis + addTimeMillis
  }

  def getCount: Long = count
  def incCount {
    count += 1
  }

  def addCount(addCount: Long) {
    count += addCount
  }

  def getTimeSeconds: Double = timeMillis / 1000.0
}

class ProcessStopWatch(id: String) {

  var isRunning: Boolean = false
  var startTimeMillis: Long = 0
  var totalTimeMillis: Long = 0
  var taskCount: Int = 0
  var lastTaskInfo: TaskInfo = null
  var currentTaskName: String = null
  var taskList: mutable.Buffer[TaskInfo] = new mutable.ListBuffer[TaskInfo]()

  def start(taskName: String) = {
    if (isRunning)
      //throw new IllegalStateException("Can't start ProcessStopWatch: it's already running")
      stop
    startTimeMillis = System.currentTimeMillis
    isRunning = true
    currentTaskName = taskName
  }

  def stop {
    stop()
  }

  def stop(count: Long = 1) {
    if (isRunning) {

      val lastTime: Long = System.currentTimeMillis() - startTimeMillis
      totalTimeMillis = totalTimeMillis + lastTime;

      lastTaskInfo = new TaskInfo(currentTaskName, lastTime, count)
      taskList.append(lastTaskInfo)
      taskCount = taskCount + 1
      isRunning = false
      currentTaskName = null
    }
    //else throw new IllegalStateException("Can't stop ProcessStopWatch: it's not running")
  }

  def shortSummary: String = {
    "StopWatch '" + id + "': running time (millis) = " + totalTimeMillis
  }

  def getTotalTimeSeconds = totalTimeMillis / 1000.0

  def combineTaskList: mutable.Buffer[TaskInfo] = {
    val m: mutable.Map[String, TaskInfo] = new mutable.HashMap[String, TaskInfo]()
    taskList.foreach { task =>
      m.get(task.getTaskName) match {
        case Some(ti) =>
          ti.addTimeMillis(task.getTimeMillis)
          ti.addCount(task.getCount)
        case None =>
          m.put(task.getTaskName, new TaskInfo(task.getTaskName, task.getTimeMillis))
      }
    }
    var taskListTmp: mutable.Buffer[TaskInfo] = new mutable.ListBuffer[TaskInfo]()
    m.foreach {
      case (key, value) => taskListTmp.append(value)
    }
    taskListTmp
  }

  def prettyPrint: String = {
    val taskListTmp: mutable.Buffer[TaskInfo] = combineTaskList
    var sb: StringBuilder = new StringBuilder(shortSummary)
    sb.append('\n')
    sb.append("-----------------------------------------\n")
    sb.append("ms % count Task name\n")
    sb.append("-----------------------------------------\n")
    var nf: NumberFormat = NumberFormat.getNumberInstance()
    nf.setMinimumIntegerDigits(5)
    nf.setGroupingUsed(false)
    var pf: NumberFormat = NumberFormat.getPercentInstance()
    pf.setMinimumIntegerDigits(3)
    pf.setGroupingUsed(false)
    taskListTmp.foreach { task =>
      sb.append(nf.format(task.getTimeMillis)).append(" ")
      sb.append(pf.format(task.getTimeSeconds / getTotalTimeSeconds)).append(" ")
      sb.append("%05d".format(task.getCount))
      sb.append(" ")
      sb.append(task.getTaskName).append("\n")
    }
    sb.toString()
  }

  def show: String = {
    val taskListTmp: mutable.Buffer[TaskInfo] = combineTaskList
    var sb: StringBuilder = new StringBuilder(shortSummary)
    taskListTmp.foreach { task =>
      sb.append("; [").append(task.getTaskName).append("] took ").append(task.getTimeMillis);
      var percent: Long = Math.round((100.0 * task.getTimeSeconds) / getTotalTimeSeconds);
      sb.append(" = ").append(percent).append("%");
    }
    sb.toString();
  }

}
