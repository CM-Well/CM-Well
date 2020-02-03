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
package cmwell.tracking

import cmwell.util.string.Base64._
import cmwell.util.string.Zip.{compress, decompress}

import scala.util.Try

/**
  * Created by yaakov on 3/15/17.
  */
case class TrackingId(actorId: String, createTime: Long) {
  def token: String = {
    val payload = s"$actorId|$createTime"
    encodeBase64URLSafeString(compress(payload))
  }
}

object TrackingId {
  def apply(actorId: String): TrackingId = TrackingId(actorId, System.currentTimeMillis())

  def unapply(token: String): Option[TrackingId] = {
    Try {
      val addrAndDate = decompress(decodeBase64(token)).split('|')
      require(addrAndDate.length == 2)
      TrackingId(actorId = addrAndDate(0), createTime = addrAndDate(1).toLong)
    }.toOption
  }
}
