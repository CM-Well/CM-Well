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
package controllers

import akka.actor.{Actor, ActorRef, PoisonPill}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

/**
  * Created by gilad on 6/2/15.
  */
case object GetID
case object GotIt
case class IterationState(actualEsScrollId: String,
                          withHistory: Boolean,
                          iteratorIdDispatcher: ActorRef)

class IteratorIdDispatcher(actualEsScrollId: String,
                           withHistory: Boolean,
                           ttl: FiniteDuration)
    extends Actor {

  var cancelable = context.system.scheduler.scheduleOnce(ttl, self, PoisonPill)

  override def receive: Receive = {
    case GetID => {
      sender() ! IterationState(actualEsScrollId, withHistory, self)
      cancelable.cancel()
      cancelable =
        context.system.scheduler.scheduleOnce(ttl * 2, self, PoisonPill)
    }
    case GotIt => {
      cancelable.cancel()
      cancelable = context.system.scheduler.scheduleOnce(ttl, self, PoisonPill)
    }
  }
}
