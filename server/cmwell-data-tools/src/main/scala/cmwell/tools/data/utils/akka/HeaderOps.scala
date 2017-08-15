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


package cmwell.tools.data.utils.akka

import akka.http.scaladsl.model.HttpHeader

object HeaderOps {
  val CMWELL_HOSTNAME = "X-CMWELL-Hostname"
  val CMWELL_POSITION = "X-CM-WELL-POSITION"
  val CMWELL_N        = "X-CM-WELL-N"
  val CMWELL_RT       = "X-CMWELL-RT"

  def getHeader(name: String)(headers: Seq[HttpHeader]) = headers.find(_.name == name)

  def getHeaderValue(header: Option[HttpHeader]) = header match {
    case None => "NA"
    case Some(HttpHeader(_, value)) => value
  }

  val getHostname    = getHeader(CMWELL_HOSTNAME) _
  val getPosition    = getHeader(CMWELL_POSITION) _
  val getNumInfotons = getHeader(CMWELL_N) _
  val getResponseTime = getHeader(CMWELL_RT) _

  val getHostnameValue    = getHostname andThen getHeaderValue
  val getPositionValue    = getPosition andThen getHeaderValue
  val getNumInfotonsValue = getNumInfotons andThen getHeaderValue
  val getResponseTimeValue = getResponseTime andThen getHeaderValue
}
