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


package filters

import javax.inject._

import akka.stream.Materializer
import controllers.NbgToggler
import play.api.mvc.{Filter, RequestHeader, Result}

import scala.concurrent.{ExecutionContext, Future}

class HeadersFilter @Inject()(nbgToggler: NbgToggler) (implicit override val mat: Materializer,ec: ExecutionContext) extends Filter {

  def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {
    val before = System.currentTimeMillis()
    next(request).map { result =>
      val after = System.currentTimeMillis()
      val bgImpl = if(request.getQueryString("nbg").exists(!_.equalsIgnoreCase("false")) || nbgToggler.get) "N" else "O"
      result.withHeaders(
        "X-CMWELL-Hostname" -> cmwell.util.os.Props.machineName,
        "X-CMWELL-Version" -> cmwell.util.build.BuildInfo.version,
        "X-CMWELL-RT" -> (after-before).toString,
        "X-CMWELL-BG" -> bgImpl)
    }
  }
}
