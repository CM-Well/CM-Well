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
package cmwell.tools.data.downloader.consumer

sealed trait ConsumeState
case class SuccessState(fails: Int) extends ConsumeState // normal state
case class LightFailure(fails: Int, successesInRow: Int) extends ConsumeState // a few errors
case class HeavyFailure(successesInRow: Int) extends ConsumeState // a lot of errors

/**
  * State machine of consume states
  *
  * @see [[ConsumeState]]
  */
object ConsumeStateHandler {
  val successToLightFailureThreshold = 2
  val lightToHeavyFailureThreshold = 2
  val lightFailureToSuccessThreshold = 15
  val heavyFailureToLightFailureThreshold = 15

  /**
    * Determines the next consume state after a failure event
    * @param state current consume state
    * @return next consume state after failure event
    */
  def nextFailure(state: ConsumeState): ConsumeState = state match {
    case SuccessState(fails) if fails == successToLightFailureThreshold               => LightFailure(0, 0)
    case SuccessState(fails)                                                          => SuccessState(fails = fails + 1) // still in success
    case LightFailure(fails, successesInRow) if fails == lightToHeavyFailureThreshold => HeavyFailure(0)
    case LightFailure(fails, successesInRow)                                          => LightFailure(fails = fails + 1, successesInRow = 0)
    case HeavyFailure(successesInRow)                                                 => HeavyFailure(successesInRow = 0)
  }

  /**
    * Determines the next consume state after a success event
    * @param state current consume state
    * @return next consume state after success event
    */
  def nextSuccess(state: ConsumeState): ConsumeState = state match {
    case s @ SuccessState(_)                                                                         => s
    case s @ LightFailure(fails, successesInRow) if successesInRow == lightFailureToSuccessThreshold => SuccessState(0)
    case s @ LightFailure(fails, successesInRow)                                                     => LightFailure(0, successesInRow + 1)
    case HeavyFailure(successesInRow) if successesInRow == heavyFailureToLightFailureThreshold       => LightFailure(0, 0)
    case HeavyFailure(successesInRow)                                                                => HeavyFailure(successesInRow + 1)
  }
}
