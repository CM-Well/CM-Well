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
package controllers

import akka.actor.{Actor, ActorRef, PoisonPill}
import cmwell.fts._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

/**
  * Created by gilad on 6/2/15.
  */
case object GetID
sealed trait IterationStateInput
case class ScrollInput(actualEsScrollId: String) extends IterationStateInput
case class StartScrollInput(pathFilter: Option[PathFilter],
                            fieldFilters: Option[FieldFilter],
                            datesFilter: Option[DatesFilter],
                            paginationParams: PaginationParams,
                            scrollTTL: Long,
                            withHistory: Boolean,
                            withDeleted: Boolean,
                            withData: Boolean) extends IterationStateInput

case class IterationState(iterationStateInput: IterationStateInput, withHistory: Boolean, iteratorIdDispatcher: ActorRef)

class IteratorIdDispatcher(iterationStateInput: IterationStateInput, withHistory: Boolean, ttl: FiniteDuration) extends Actor {

  var cancelable = context.system.scheduler.scheduleOnce(ttl, self, PoisonPill)

  override def receive: Receive = {
    case GetID => {
      sender() ! IterationState(iterationStateInput, withHistory, self)
      cancelable.cancel()
      context.stop(self)
    }
  }
}
