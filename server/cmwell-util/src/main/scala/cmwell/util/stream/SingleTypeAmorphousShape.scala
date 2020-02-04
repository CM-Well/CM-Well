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
package cmwell.util.stream

import akka.stream.{Inlet, Outlet, Shape}

import scala.collection.immutable

/**
  * Proj: server
  * User: gilad
  * Date: 9/5/17
  * Time: 6:02 AM
  */
case class SingleTypeAmorphousShape[I, O](inlets: immutable.Seq[Inlet[I]], outlets: immutable.Seq[Outlet[O]])
    extends Shape {
  override def deepCopy() = SingleTypeAmorphousShape(inlets.map(_.carbonCopy()), outlets.map(_.carbonCopy()))
}
