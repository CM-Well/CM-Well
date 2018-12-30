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
package cmwell.dc

import akka.util.ByteString

package object stream {
  val empty = ByteString("")
  val endln = ByteString("\n")
  val tab = ByteString("\t")
  val space: Char = 32
  val ii = ByteString("/ii/")
  val cmwellPrefix = "cmwell:/"
  val lessThan = ByteString("<")
  val cmwellBlankNodeIdRegex = "_:B(A[a-f0-9X]{32,})".r
}
