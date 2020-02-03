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
package filters

import javax.inject._

import akka.stream.Materializer
import cmwell.ws.util.TypeHelpers
import play.api.libs.typedmap.TypedKey
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Proj: server
  * User: gilad
  * Date: 10/24/17
  * Time: 8:04 AM
  */
class GeneralAttributesFilter @Inject()(implicit val mat: Materializer, ec: ExecutionContext)
    extends Filter
    with TypeHelpers {
  override def apply(f: RequestHeader => Future[Result])(rh: RequestHeader) = {
    f(
      rh.addAttr(Attrs.RequestReceivedTimestamp, System.currentTimeMillis())
        .addAttr(Attrs.UserIP, rh.headers.get("X-Forwarded-For").getOrElse(rh.remoteAddress)) // TODO: might be good to add ip to logs when user misbehaves
    )
  }
}

object Attrs {
  val RequestReceivedTimestamp: TypedKey[Long] = TypedKey.apply[Long]("RequestReceivedTimestamp")
  val UserIP: TypedKey[String] = TypedKey.apply[String]("UserIP")
  val UserName: TypedKey[String] = TypedKey.apply[String]("UserName")
}
