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
package cmwell.util

import scala.language.higherKinds
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.util.{Success, Try}

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 8/1/13
  * Time: 9:59 AM
  * To change this template use File | Settings | File Templates.
  */
package object exceptions {

  def trySequence[A, M[X] <: TraversableOnce[X]](
    in: M[Try[A]]
  )(implicit cbf: CanBuildFrom[M[Try[A]], A, M[A]]): Try[M[A]] = {
    in.foldLeft(Success(cbf(in)): Try[mutable.Builder[A, M[A]]]) { (tr, ta) =>
        {
          for {
            r <- tr
            a <- ta
          } yield r += a
        }
      }
      .map(_.result())
  }

  def stackTraceToString(t: Throwable): String = {
    val w = new java.io.StringWriter()
    val pw = new java.io.PrintWriter(w)
    t.printStackTrace(pw)
    val resp = w.toString
    w.close()
    pw.close()
    resp
  }
}
