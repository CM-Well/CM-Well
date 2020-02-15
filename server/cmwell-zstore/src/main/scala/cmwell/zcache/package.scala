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
package cmwell

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by yaakov on 12/14/16.
  */
package object zcache {
  def l1l2[K, V](task: K => Future[V])(
    digest: K => String,
    deserializer: Array[Byte] => V,
    serializer: V => Array[Byte],
    isCachable: V => Boolean = (_: V) => true
  )(ttlSeconds: Int = 10, pollingMaxRetries: Int = 5, pollingInterval: Int = 1, l1Size: Int = 1024)(
    zCache: ZCache
  )(implicit ec: ExecutionContext): K => Future[V] = {

    def l2 =
      zCache.memoize(task)(digest, deserializer, serializer, isCachable)(ttlSeconds, pollingMaxRetries, pollingInterval)
    L1Cache.memoize(l2)(digest, isCachable)(l1Size, ttlSeconds)
  }
}
