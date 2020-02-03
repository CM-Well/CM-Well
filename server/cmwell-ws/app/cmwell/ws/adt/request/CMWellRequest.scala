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
package cmwell.ws.adt.request

import play.api.mvc.RequestHeader
import cmwell.ws.util.RequestHelpers.cmWellBase

sealed trait CMWellRequest {

  protected def requestHeader: RequestHeader
  protected[this] def queryParams: Map[String, Seq[String]]

  lazy val path: String = wsutil.normalizePath(requestHeader.path)

  // order of ++ is important. we want to give precedence to parameters in payload
  // and only take defaults from query params if payload don't have it.
  // i.e: keys in `queryParams` will override same keys in `requestHeader.queryString`
  lazy val queryParameters: Map[String, Seq[String]] = requestHeader.queryString ++ queryParams
}

final class CreateConsumer private (protected override val requestHeader: RequestHeader,
                                    protected[this] override val queryParams: Map[String, Seq[String]])
    extends CMWellRequest
object CreateConsumer {
  def apply(rh: RequestHeader, qp: Map[String, Seq[String]]) = new CreateConsumer(rh, qp)
  def unapply(cc: CreateConsumer) = Some((cc.path, cc.queryParameters))
}

final class Search private (protected override val requestHeader: RequestHeader,
                            protected[this] override val queryParams: Map[String, Seq[String]])
    extends CMWellRequest {
  private lazy val base = cmWellBase(requestHeader)
  private lazy val host = requestHeader.host
  private lazy val uri = requestHeader.uri
}
object Search {
  def apply(rh: RequestHeader, qp: Map[String, Seq[String]]) = new Search(rh, qp)
  def unapply(s: Search) = Some((s.path, s.base, s.host, s.uri, s.queryParameters))
}
