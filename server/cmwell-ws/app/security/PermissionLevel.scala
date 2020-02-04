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
package object security {

  object PermissionLevel extends Enumeration {
    type PermissionLevel = Value
    val Read, Write = Value

    private val readOperations = Set("search", "create-consumer")

    def apply(verb: String) = verb match {
      case "PUT" | "POST" | "DELETE" => Write
      case _                         => Read
    }

    def apply(verb: String, op: Option[String]) = (verb, op) match {
      case ("POST", Some(operation)) if readOperations(operation.toLowerCase) => Read
      case ("PUT", _) | ("POST", _) | ("DELETE", _)                           => Write
      case _                                                                  => Read
    }
  }
}
