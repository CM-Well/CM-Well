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
package security

import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by yaakov on 5/17/16.
  */
class NoncesManager extends Actor with LazyLogging {
  private var nonces = Set.empty[String]

  override def receive: Receive = {
    case AddNonce(nonce)                      => nonces += nonce
    case ConsumeNonce(nonce) if nonces(nonce) => nonces -= nonce; sender ! NonceConsumed
    case ConsumeNonce(nonce)                  => sender ! NonceNotExist
  }
}

case class AddNonce(nonce: String)
case class ConsumeNonce(nonce: String)

trait NonceStatus
case object NonceNotExist extends NonceStatus
case object NonceConsumed extends NonceStatus
