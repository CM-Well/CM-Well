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

import akka.stream.ThrottleMode.{Enforcing, Shaping}
import akka.stream.stage.GraphStage
import akka.stream.stage._
import akka.stream.{Attributes, RateExceededException, ThrottleMode, _}
import akka.util.NanoTimeTokenBucket

import scala.concurrent.duration.{FiniteDuration, _}

/**
  * INTERNAL API
  */
class TitForTatThrottle[T] extends GraphStage[FlowShape[T, T]] {

  val in = Inlet[T]("TitForTatThrottle.in")
  val out = Outlet[T]("TitForTatThrottle.out")
  override val shape = FlowShape(in, out)

  // There is some loss of precision here because of rounding, but this only happens if nanosBetweenTokens is very
  // small which is usually at rates where that precision is highly unlikely anyway as the overhead of this stage
  // is likely higher than the required accuracy interval.
//  private val nanosBetweenTokens = per.toNanos / cost
  private val timerName: String = "ThrottleTimer"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {
//    private val tokenBucket = new NanoTimeTokenBucket(maximumBurst, nanosBetweenTokens)

    var willStop = false
    var currentElement: T = _

    // This scope is here just to not retain an extra reference to the handler below.
    // We can't put this code into preRestart() because setHandler() must be called before that.
    {
      val handler = new InHandler with OutHandler {
        var timeOfPreviousElmement = System.currentTimeMillis()

        override def onUpstreamFinish(): Unit =
          if (isAvailable(out) && isTimerActive(timerName)) willStop = true
          else completeStage()

        override def onPush(): Unit = {
          val elem = grab(in)
          val now = System.currentTimeMillis()
          val delayMillis = now - timeOfPreviousElmement
          timeOfPreviousElmement = now

          if (delayMillis == 0L) push(out, elem)
          else {
            currentElement = elem
            System.err.println(s"scheduled push in ${delayMillis.milliseconds}")
            scheduleOnce(timerName, delayMillis.milliseconds)
          }
        }

        override def onPull(): Unit = pull(in)
      }

      setHandler(in, handler)
      setHandler(out, handler)
      // After this point, we no longer need the `handler` so it can just fall out of scope.
    }

    override protected def onTimer(key: Any): Unit = {
      push(out, currentElement)
      currentElement = null.asInstanceOf[T]
      if (willStop) completeStage()
    }

  }

  override def toString = "Throttle"
}
