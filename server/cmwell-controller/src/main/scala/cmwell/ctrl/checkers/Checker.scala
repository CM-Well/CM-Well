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
package cmwell.ctrl.checkers

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Created by michael on 12/3/14.
  */
case class StateStore(size: Int) extends LazyLogging {
  private[this] val s = new scala.collection.mutable.Queue[ComponentState]()

  def add(cs: ComponentState): Unit = {
    s.enqueue(cs)
    if (s.size > size) s.dequeue()
    logger.debug(s"StateStore: $s, max-size: $size, current-size: ${s.size}")
  }

  def getLastStates(num: Int): Vector[ComponentState] = {
    s.reverse.take(num).toVector
  }

  def reset: Unit = {
    s.clear()
  }
}

trait Checker {
  val storedStates: Int = 5
  private lazy val states = StateStore(storedStates)
  def check: Future[ComponentState]
  def storeState(cs: ComponentState): Unit = {
    states.add(cs)
  }

  def getLastStates(num: Int) = states.getLastStates(num)
  def resetStates = states.reset
}

trait RestarterChecker {
  def restartAfter: FiniteDuration
  def doRestart: Unit
}
