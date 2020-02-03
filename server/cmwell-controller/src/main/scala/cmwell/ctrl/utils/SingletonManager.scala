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
//package cmwell.ctrl.utils
//
//import akka.actor.ActorRef
//import scala.concurrent.{Future, Await}
//import scala.concurrent.duration._
//import k.grid._
//import scala.concurrent.ExecutionContext.Implicits.global
///**
// * Created by michael on 2/8/15.
// */
//
//case object ActivateSingleton
//trait SingletonManager {
//
//  Grid.system.scheduler.schedule(0 seconds, 5 seconds, getRef, ActivateSingleton)
//
//  private[this] var active = false
//  protected def setActive = active = true
//
//  def getRef : ActorRef
//  def isActive = active
//
//
//}
