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
package cmwell.util.http

import java.net.URLEncoder.encode

object StringPath {

  import scala.language.implicitConversions

  implicit def stringPath2String(sp: StringPath): String = sp.url

  //e.g StringPath("http://localhost:9000")
  def apply(withProtocol: String) = new StringPath(withProtocol)

  //e.g StringPath.host("localhost:9000")
  def host(domainAndPort: String) = new StringPath("http://" + domainAndPort)

  //e.g StringPath.host("localhost",9000)
  def host(domain: String, port: Int) = new StringPath("http://" + domain + s":$port")

  //e.g StringPath.sHost("localhost:9000")
  def sHost(domainAndPort: String) = new StringPath("https://" + domainAndPort)

  //e.g StringPath.sHost("localhost",9000)
  def sHost(domain: String, port: Int) = new StringPath("https://" + domain + s":$port")
}

class StringPath private (val url: String) {
  def /(pathPart: String) = new StringPath(url + s"/${encode(pathPart, "UTF-8")}")
  def h(fragment: String) = new StringPath(url + s"#${encode(fragment, "UTF-8")}")
  @inline def ⋕(fragment: String) =
    h(fragment) // other hash unicode look-alikes: '⌗','♯' ('#' - %23, is illegal as a method name in scala...)
}
