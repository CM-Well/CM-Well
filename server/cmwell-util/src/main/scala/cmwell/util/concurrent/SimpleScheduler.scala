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
package cmwell.util.concurrent

import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor}

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

/**
  * Created by gilad on 12/3/15.
  */
object SimpleScheduler extends LazyLogging {
  private[this] lazy val timer = {
    val executor = new ScheduledThreadPoolExecutor(1)
    executor.setRemoveOnCancelPolicy(true)
    executor.asInstanceOf[ScheduledExecutorService]
  }

  //method is private, since we must keep execution on the expense of out timer thread to be as limited as possible.
  //this method can be used if and only if we know `body` is a safe and small job.
  private[util] def scheduleInstant[T](duration: FiniteDuration)(body: => T)(implicit executionContext: ExecutionContext) = {
    val p = Promise[T]()
    val cancellable = timer.schedule(
      new Runnable {
        override def run(): Unit = {
          // body must not be expensive to compute since it will be run in our only timer thread expense.
          p.complete(Try(body))
        }
      },
      duration.toMillis,
      java.util.concurrent.TimeUnit.MILLISECONDS
    )
    p.future.andThen { case _ => cancellable.cancel(false) }
  }

  def scheduleAtFixedRate(initialDelay: FiniteDuration, period: FiniteDuration, mayInterruptIfRunning: Boolean = false)(
    task: => Any
  )(implicit executionContext: ExecutionContext): Cancellable = {
    // memoize runnable task
    val runnable: Runnable = new Runnable {
      override def run(): Unit = Try(task).failed.foreach { err =>
        logger.error("schedueled task failed", err)
      }
    }

    //prepare returns `this` anyway in all seen cases... (can't see why NOT do this)
    val ec = executionContext.prepare()

    val cancellable = timer.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = ec.execute(runnable)
    }, initialDelay.toMillis, period.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)

    new Cancellable {
      override def cancel() = cancellable.cancel(mayInterruptIfRunning)
    }
  }

  def schedule[T](duration: FiniteDuration)(body: => T)
                 (implicit executionContext: ExecutionContext): Future[T]= {
    val p = Promise[T]()
    val cancellable = timer.schedule(
      new Runnable {
        override def run(): Unit = {
          // body may be expensive to compute, and must not be run in our only timer thread expense,
          // so we compute the task inside a `Future` and make it run on the expense of the given executionContext.
          p.completeWith(Future(body)(executionContext))
        }
      },
      duration.toMillis,
      java.util.concurrent.TimeUnit.MILLISECONDS
    )
    p.future.andThen { case _ => cancellable.cancel(false) }
  }

  def scheduleFuture[T](duration: Duration)(body: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val p = Promise[T]()
    val cancellable = timer.schedule(new Runnable {
      override def run(): Unit = p.completeWith(body)
    }, duration.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    p.future.andThen { case _ => cancellable.cancel(false) }
  }
}

trait Cancellable {
  def cancel(): Boolean
}
