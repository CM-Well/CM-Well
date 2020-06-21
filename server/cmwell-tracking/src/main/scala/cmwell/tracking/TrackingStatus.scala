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
package cmwell.tracking

/**
  * Created by yaakov on 3/15/17.
  */
sealed trait TrackingStatus

case object InProgress extends TrackingStatus
case object Done extends TrackingStatus
case object Failed extends TrackingStatus

final case class PartialDone(completed: Int, total: Int) extends TrackingStatus {
  override def toString: String = s"PartialDone($completed/$total)"
}

final case class Evicted(reason: String) extends TrackingStatus

object TrackingStatus {
  private val partialDoneRegex = "PartialDone\\((\\d+)/(\\d+)\\)".r
  private val evictedRegex = "Evicted\\((.*?)\\)".r

  def apply(value: String): TrackingStatus = value match {
    case "InProgress"                            => InProgress
    case "Done"                                  => Done
    case evictedRegex(reason) if reason.nonEmpty => Evicted(reason)
    case "Failed"                                => Failed
    case partialDoneRegex(completed, total)      => PartialDone(completed.toInt, total.toInt)
    case other                                   => throw new IllegalArgumentException(s"$other is not a valid TrackingStatus")
  }

  def isFinal(ts: TrackingStatus): Boolean = ts match {
    case InProgress | _: PartialDone => false
    case _                           => true
  }
}

final case class PathStatus(path: String, status: TrackingStatus)
