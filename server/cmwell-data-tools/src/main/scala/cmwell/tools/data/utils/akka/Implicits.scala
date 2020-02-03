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
import akka.stream.{ActorMaterializer, Materializer}

/**
  * Created by matan on 2/2/16.
  */
object Implicits {
  implicit lazy val system: ActorSystem = ActorSystem("reactive-tools-system")
  implicit lazy val mat: Materializer = ActorMaterializer()
}
