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
package cmwell.common

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import cmwell.zstore.ZStore

/**
  * Created by israel on 14/03/2017.
  */
trait OffsetsService {

  def read(id: String): Option[Long]
  def readWithTimestamp(id: String): Option[PersistedOffset]
  def writeAsync(id: String, offset: Long): Future[Unit]
}

case class PersistedOffset(offset: Long, timestamp: Long)

class ZStoreOffsetsService(zStore: ZStore) extends OffsetsService {

  override def readWithTimestamp(id: String): Option[PersistedOffset] =
    Await.result(zStore.getOpt(id, dontRetry = true), 10.seconds).map { payload =>
      //todo: this is a check to allow backward compatibility until all clusters` persisted offsets will contain also timestamp
      if (payload.length == 8)
        PersistedOffset(ByteBuffer.wrap(payload).getLong, -1)
      else {
        val s = new String(payload, StandardCharsets.UTF_8)
        val (offset, timestamp) = cmwell.util.string.splitAtNoSep(s, ',')
        PersistedOffset(offset.toLong, timestamp.toLong)
      }
    }

  override def read(id: String): Option[Long] =
    Await.result(zStore.getOpt(id, dontRetry = true), 10.seconds).map { payload =>
      //todo: this is a check to allow backward compatibility until all clusters` persisted offsets will contain also timestamp
      if (payload.length == 8)
        ByteBuffer.wrap(payload).getLong
      else {
        val s = new String(payload, StandardCharsets.UTF_8)
        s.substring(0, s.indexOf(',')).toLong
      }
    }
//    Await.result(zStore.getStringOpt(id), 10.seconds).map(s => s.substring(0, s.indexOf(',')).toLong)
//    Await.result(zStore.getLongOpt(id), 10.seconds)

  override def writeAsync(id: String, offset: Long): Future[Unit] =
    zStore.putString(id, s"$offset,${System.currentTimeMillis}", batched = true)
}
