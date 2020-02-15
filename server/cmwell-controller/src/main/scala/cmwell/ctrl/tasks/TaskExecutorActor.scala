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
package cmwell.ctrl.tasks

import akka.actor.{Actor, ActorRef}
import akka.actor.Actor.Receive
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 3/9/16.
  */
trait TaskStatus
case object InProgress extends TaskStatus
case object Complete extends TaskStatus
case object Failed extends TaskStatus

object TaskExecutorActor {
  val name = "TaskExecutorActor"
}

class TaskExecutorActor extends Actor with LazyLogging {
  implicit val timeout = Timeout(15.seconds)
  private var status: TaskStatus = _
  private var s: ActorRef = _
  private case class TaskFinished(status: TaskStatus)
  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {}

  override def receive: Receive = {
    case t: Task => {
      logger.info(s"Starting task: $t")
      s = sender()
      status = InProgress
      t.exec.onComplete {
        case Success(tr) => {
          tr match {
            case TaskSuccessful =>
              self ! TaskFinished(Complete)
              s ! TaskSuccessful
            case TaskFailed =>
              self ! TaskFinished(Failed)
              s ! TaskFailed
          }
        }
        case Failure(err) =>
          self ! TaskFinished(Failed)
          s ! TaskFailed
      }
    }
    case TaskFinished(stat) => {
      status = stat
    }
  }
}
