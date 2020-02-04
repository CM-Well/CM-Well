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
package cmwell.tools.data.utils.akka

import akka.actor.ActorSystem
import akka.http.scaladsl._
import akka.stream.Materializer
import cmwell.tools.data.utils.logging.LabelId

object HttpConnections extends DataToolsConfig {

  def outgoingConnection(host: String, port: Int, protocol: String = "http")(implicit system: ActorSystem,
                                                                             label: Option[LabelId] = None) = {
    val userAgent = label.fold(s"cmwell-data-tools using akka-http/${config.getString("akka.version")}")(
      l => s"cmwell-data-tools ${l.id}"
    )

    val settings = AkkaUtils.generateClientConnectionSettings(userAgent)

    protocol match {
      case "https" => Http().outgoingConnectionHttps(host, port, settings = settings)
      case _       => Http().outgoingConnection(host, port, settings = settings)
    }
  }

  def newHostConnectionPool[T](host: String, port: Int, protocol: String = "http")(implicit system: ActorSystem,
                                                                                   mat: Materializer,
                                                                                   label: Option[LabelId] = None) = {
    val userAgent = label.fold(s"cmwell-data-tools using akka-http/${config.getString("akka.version")}")(
      l => s"cmwell-data-tools ${l.id}"
    )

    val settings = AkkaUtils.generateConnectionPoolSettings(Some(userAgent))

    protocol match {
      case "https" => Http().newHostConnectionPoolHttps[T](host, port, settings = settings)
      case _       => Http().newHostConnectionPool[T](host, port, settings = settings)
    }
  }

  def cachedHostConnectionPool[T](host: String, port: Int, protocol: String = "http")(implicit system: ActorSystem,
                                                                                      mat: Materializer,
                                                                                      label: Option[LabelId] = None) = {
    val userAgent = label.fold(s"cmwell-data-tools using akka-http/${config.getString("akka.version")}")(
      l => s"cmwell-data-tools ${l.id}"
    )

    val settings = AkkaUtils.generateConnectionPoolSettings(Some(userAgent))

    protocol match {
      case "https" => Http().cachedHostConnectionPoolHttps[T](host, port, settings = settings)
      case _       => Http().cachedHostConnectionPool[T](host, port, settings = settings)
    }
  }
}
