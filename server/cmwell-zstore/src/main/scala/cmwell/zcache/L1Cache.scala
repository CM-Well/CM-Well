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
package cmwell.zcache

import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  *
  * L1Cache is a JVM-Local Cache can be used as a wrapper for zCache, in an L1-L2 Style.
  * Based on passive Guava Cache, keeping Futures[T] as values,
  * which might be completed or in-flight heavy calculations.
  *
  */
object L1Cache {
  def memoize[K, V](task: K => Future[V])(
    digest: K => String,
    isCachable: V => Boolean = (_: V) => true
  )(l1Size: Int = 1024, ttlSeconds: Int = 10)(implicit ec: ExecutionContext): K => Future[V] = {

    val cache: Cache[String, Future[V]] =
      CacheBuilder.newBuilder().maximumSize(l1Size).expireAfterWrite(ttlSeconds, TimeUnit.SECONDS).build()

    (input: K) =>
      {
        val key = digest(input)
        Option(cache.getIfPresent(key)) match {
          case Some(cachedFuture) =>
            cachedFuture.asInstanceOf[Future[V]]
          case None => {
            val fut = task(input)
            cache.put(key, fut)
            fut.andThen {
              case Success(v) => if (!isCachable(v)) cache.invalidate(key)
              case Failure(_) =>
                cache.getIfPresent(key) match {
                  case `fut` => cache.invalidate(key)
                  case _     => //Do Nothing
                }
            }
          }
        }
      }
  }

  def memoizeWithCache[K, V](task: K => Future[V])(digest: K => String, isCachable: V => Boolean = (_: V) => true)(
    cache: Cache[String, Future[V]]
  )(implicit ec: ExecutionContext): K => Future[V] = { (input: K) =>
    {
      val key = digest(input)
      Option(cache.getIfPresent(key)).getOrElse {
        val fut = task(input)
        cache.put(key, fut)
        fut.andThen {
          case Success(v) =>
            if (!isCachable(v)) cache.getIfPresent(key) match {
              case `fut` => cache.invalidate(key)
              case _     => //Do Nothing
            }
          case Failure(_) =>
            cache.getIfPresent(key) match {
              case `fut` => cache.invalidate(key)
              case _     => //Do Nothing
            }
        }
      }
    }
  }
}
