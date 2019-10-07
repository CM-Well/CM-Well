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

package cmwell.util.testSuitHelpers.test

import java.util.concurrent.Executors

import com.dimafeng.testcontainers.{Container, LazyContainer}
import org.junit.runner.Description

import scala.concurrent.{Await, ExecutionContext, Future}

class MultipleContainersParallelExecution private(containers: Seq[LazyContainer[_]]) extends Container {
  implicit val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(10))
  import scala.concurrent.duration._

  override def finished()(implicit description: Description): Unit = {
    val f = Future.traverse(containers)(lazyContainer => Future(lazyContainer.finished()(description)))
    Await.ready(f, 5.minutes)
  }

  override def succeeded()(implicit description: Description): Unit = {
    val f = Future.traverse(containers)(lazyContainer => Future(lazyContainer.succeeded()(description)))
    Await.ready(f, 5.minutes)
  }

  override def starting()(implicit description: Description): Unit = {
    val f = Future.traverse(containers)(lazyContainer => Future(lazyContainer.starting()(description)))
    Await.ready(f, 5.minutes)
  }

  override def failed(e: Throwable)(implicit description: Description): Unit = {
    val f = Future.traverse(containers)(lazyContainer => Future(lazyContainer.failed(e)(description)))
    Await.ready(f, 5.minutes)
  }
}

object MultipleContainersParallelExecution {

  def apply(containers: LazyContainer[_]*): MultipleContainersParallelExecution = new MultipleContainersParallelExecution(containers)
}
