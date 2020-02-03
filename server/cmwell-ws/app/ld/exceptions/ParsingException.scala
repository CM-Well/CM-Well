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
package cmwell.web.ld.exceptions

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 7/18/13
  * Time: 4:03 PM
  * To change this template use File | Settings | File Templates.
  */
class ParsingException(msg: String, t: Throwable = null) extends RuntimeException(msg, t)

class UnretrievableIdentifierException(msg: String, t: Throwable = null) extends ParsingException(msg, t)

sealed trait ServerComponentNotAvailableException extends Exception
object ServerComponentNotAvailableException {

  def apply(msg: String) = new Exception(msg) with ServerComponentNotAvailableException
  def apply(msg: String, cause: Throwable) = new Exception(msg, cause) with ServerComponentNotAvailableException
}

class JsonParsingException(msg: String) extends ParsingException(msg)
