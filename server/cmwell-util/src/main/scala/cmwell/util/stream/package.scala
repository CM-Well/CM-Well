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
package cmwell.util

import akka.stream.scaladsl.Source
import scala.collection.SeqLike
import scala.collection.generic.CanBuildFrom

package object stream {

  import scala.language.higherKinds

  def mergeSourcesMat[T, M, Coll[_]](sources: Coll[Source[T, M]])(
    implicit ev: Coll[T] <:< SeqLike[T, Coll[T]],
    cbf: CanBuildFrom[Coll[T], T, Coll[T]]
  ): Source[T, Coll[M]] = {
    ???
  }
}
