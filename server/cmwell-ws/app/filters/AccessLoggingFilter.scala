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
import play.api.Logger
import play.api.mvc._
import scala.concurrent.{ExecutionContext, Future}

class AccessLoggingFilter @Inject()(implicit val mat: Materializer, ec: ExecutionContext) extends Filter {

  val accessLogger = Logger("access")

  def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {
    val startTime = request.attrs(Attrs.RequestReceivedTimestamp)
    next(request).map { result =>
      val endTime = System.currentTimeMillis
      val reqTime = endTime - startTime
      val ip = request.headers.get("X-Forwarded-For").getOrElse("N/A")
      val msg = s"method=${request.method} uri=${request.uri} remote-address=${request.remoteAddress} " +
        s"status=${result.header.status} process-time=$reqTime x-forwarded-for=${ip}";
      lazy val headers = request.headers.headers.map(h => h._1 + ":" + h._2).mkString(" headers=[", ",", "]")

      if (result.header.status < 400) accessLogger.info(msg)
      else if (result.header.status != 500) accessLogger.warn(msg + headers)
      else accessLogger.error(msg + headers)

      result.withHeaders("X-CMWELL-RT" -> s"${reqTime}ms")
    }
  }
}
