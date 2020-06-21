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
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by michael on 1/13/15.
  */
object Retry {
  def apply[T](block: => T, retries: Int = 3, delay: Int = 5000): Try[T] = {
    val r = cmwell.util.concurrent.retry(retries + 1,delay.millis)(Future.successful(Try(block)))(ExecutionContext.global)
    val f = r.transform(_.flatten)(ExecutionContext.global)
    f.value.getOrElse(Try(Await.result(f, Duration.Inf)))
  }
}
