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
package logic.services

sealed trait ServiceDefinition {
  val route: String
}

case class RedirectionService(override val route: String, srcPattern: String, replaceFunc: String => String) extends ServiceDefinition


// For future use. See: https://github.com/CM-Well/CM-Well/tree/master/blps/blp-703-servies.md

//sealed abstract class LogicService(override val route: String, logicPath: String,
//                                   args: Array[String], returnType: ServiceReturnType) extends ServiceDefinition
//
//case class Source(override val route: String, logicPath: String,
//                  args: Array[String], returnType: ServiceReturnType) extends LogicService(route, logicPath, args, returnType)
//case class Binary(override val route: String, logicPath: String,
//                  args: Array[String], returnType: ServiceReturnType) extends LogicService(route, logicPath, args, returnType)
//
//case class ServiceReturnType(infotonType: String, mimeType: Option[String])
