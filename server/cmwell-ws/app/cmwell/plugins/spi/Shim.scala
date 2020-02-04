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
package cmwell.plugins.spi

import java.util.concurrent.Future
import org.apache.http.HttpResponse

trait Shim {
  def process(rawQueryString: String, cmwellClient: SimpleCmWellClientForShim): HttpResponse
}

trait SimpleCmWellClientForShim {
  def sp(paths: java.util.List[String], sparql: String, format: String): Future[String]
  def get(path: String, queryParams: java.util.Map[String, String]): Future[String]
}
