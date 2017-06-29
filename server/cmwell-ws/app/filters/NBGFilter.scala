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
import logic.CRUDServiceFS
import play.api.mvc.{Filter, RequestHeader, Result}

import scala.concurrent.{ExecutionContext, Future}

class NBGFilter @Inject() (implicit override val mat: Materializer,ec: ExecutionContext) extends Filter {

  override def apply(next: (RequestHeader) => Future[Result])(req: RequestHeader): Future[Result] = {
    if (req.path == "/_tbg") next(req)
    else {
      val nbg = req.queryString.keySet("nbg")
      val old = CRUDServiceFS.newBG
      CRUDServiceFS.newBG = CRUDServiceFS.newBG || nbg
      next(req).andThen {
        case _ => CRUDServiceFS.newBG = old
      }
    }
  }
}
