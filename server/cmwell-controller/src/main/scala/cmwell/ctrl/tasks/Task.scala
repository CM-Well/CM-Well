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
package cmwell.ctrl.tasks

import k.grid.Grid

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 3/9/16.
  */
trait TaskResult
case object TaskSuccessful extends TaskResult
case object TaskFailed extends TaskResult

trait Task {
  protected def cancel(prom: Promise[Unit], d: FiniteDuration) = {
    Grid.system.scheduler.scheduleOnce(d) {
      prom.failure(new Throwable("Task reached its timeout and was canceled."))
    }
  }

  def exec: Future[TaskResult]
}
