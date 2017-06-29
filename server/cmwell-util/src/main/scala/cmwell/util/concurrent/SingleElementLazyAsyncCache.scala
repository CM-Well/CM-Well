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

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait Combiner[T]{
  def combine(t1: T, t2: T): T
}
object Combiner {
  implicit val setStringCombiner = new Combiner[Set[String]] {
    override def combine(t1: Set[String], t2: Set[String]): Set[String] = t1 union t2
  }
  def replacer[T]: Combiner[T] = new Combiner[T] {
    override def combine(t1: T, t2: T) = t2
  }
}
class SingleElementLazyAsyncCache[T: Combiner](refreshThresholdInMillis: Long, initial: T = null.asInstanceOf[T])(getAsync: => Future[T])(implicit ec: ExecutionContext) {

  // element with timestamp (last update time)
  private[this] var cachedElement: Either[Future[T],(T,Long)] = Right(initial -> 0L)
  private[this] val isBeingUpdated = new AtomicBoolean(false)
  private[this] val combiner = implicitly[Combiner[T]]

  def getAndUpdateIfNeeded: Future[T] = {
    cachedElement match {
      case Left(fut) => fut
      case right@Right((elem, timestamp)) => {
        if (System.currentTimeMillis() - timestamp <= refreshThresholdInMillis) {
          Future.successful(elem)
        } else {
          if(!isBeingUpdated.compareAndSet(false, true))
            cachedElement.fold(identity, t => Future.successful(t._1))
          else {
            val p = Promise[T]()
            val rv = p.future
            cachedElement = Left(rv)
            try {
              val f = getAsync
              p.completeWith {
                f.andThen {
                  case Success(newValue) =>
                    cachedElement = Right(combiner.combine(elem,newValue) -> System.currentTimeMillis())
                  case Failure(e) =>
                    cachedElement = right
                }.andThen {
                  case _ =>
                    isBeingUpdated.set(false)
                }
              }
            } catch {
              case t: Throwable => {
                cachedElement = right
                isBeingUpdated.set(false)
                p.failure(t)
              }
            }
            rv
          }
        }
      }
    }
  }

  def getLastUpdateTime: Option[Long] = cachedElement.right.toOption.map(_._2) // todo this is a bad use of Option (None here does not mean none). Use a specific defined ADT!
}
