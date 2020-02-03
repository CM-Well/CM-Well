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
import controllers.XCmWellType
import play.api.http.HeaderNames
import play.api.mvc.{Filter, Headers, RequestHeader, Result}
import scala.concurrent.Future

class AddCharsetIfNotExistFilter @Inject()(implicit override val mat: Materializer) extends Filter {
  def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = request match {
    case XCmWellType.File()             => next(request)
    case _ if request.charset.isDefined => next(request)
    case _ => {
      val charset = request.charset.getOrElse("UTF-8")
      val contentType = request.contentType.getOrElse("text/plain")

      val headers = request.headers.headers.filterNot(_._1 == HeaderNames.CONTENT_TYPE) ++ Seq(
        HeaderNames.CONTENT_TYPE -> s"$contentType;charset=$charset"
      )
      val modifiedRequestHeader = request.withHeaders(Headers(headers: _*))

      next(modifiedRequestHeader)
    }
  }
}
