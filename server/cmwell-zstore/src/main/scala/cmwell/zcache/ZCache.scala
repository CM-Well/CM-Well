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

import cmwell.util.concurrent.unsafeRetryUntil
import cmwell.zstore.ZStore

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * ZCache is a cache wrapping of zStore.
  * Can be used as passive cache (by invoking put and get),
  * or as an active cache in a Memoization style.
  *
  * Can be wrapped with L1Cache.
  *
  */
class ZCache(zStore: ZStore) {
  def memoize[K, V](task: K => Future[V])(
    digest: K => String,
    deserializer: Array[Byte] => V,
    serializer: V => Array[Byte],
    isCachable: V => Boolean = (_: V) => true
  )(ttlSeconds: Int = 10, pollingMaxRetries: Int = 5, pollingInterval: Int = 1)(
    implicit ec: ExecutionContext
  ): K => Future[V] = { (input: K) =>
    {
      val key = digest(input)
      get(key)(deserializer, pollingMaxRetries, pollingInterval)(ec).flatMap {
        case Some(result) => Future.successful(result)
        case None => {
          put(key, Pending)(ttlSeconds)(ec)
          task(input).flatMap {
            case result if isCachable(result) => put(key, result)(serializer, ttlSeconds)(ec).map(_ => result)
            case result                       => remove(key)(ec).map(_ => result)
          }
        }
      }
    }
  }

  def get[T](key: String)(deserializer: Array[Byte] => T, pollingMaxRetries: Int, pollingInterval: Int)(
    implicit ec: ExecutionContext
  ): Future[Option[T]] = {
    def isActualValue(valueOpt: Option[Array[Byte]]) = !valueOpt.exists(_.sameElements(Pending.payload))
    unsafeRetryUntil(isActualValue, pollingMaxRetries, pollingInterval.seconds)(zStore.getOpt(key, dontRetry = true))
      .map {
        case Some(payload) if payload.sameElements(Pending.payload) => None
        case noneOrActualValue                                      => noneOrActualValue.map(deserializer)
      }
  }

  def put(key: String, state: State)(ttl: Int)(implicit ec: ExecutionContext): Future[Unit] =
    zStore.put(key, state.payload, secondsToLive = ttl, false)

  def put[T](key: String, value: T)(serializer: T => Array[Byte], ttl: Int)(implicit ec: ExecutionContext): Future[T] =
    zStore.put(key, serializer(value), secondsToLive = ttl, false).map(_ => value)

  def remove(key: String)(implicit ec: ExecutionContext): Future[Unit] = zStore.remove(key)

  trait State { val payload: Array[Byte] }
  case object Pending extends State { val payload = "PENDING".getBytes }
}
