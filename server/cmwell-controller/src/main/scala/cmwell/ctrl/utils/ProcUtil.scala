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
package cmwell.ctrl.utils

import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}
import scala.sys.process._
import scala.language.postfixOps

/**
  * Created by michael on 12/4/14.
  */
object ProcUtil extends LazyLogging {

  def executeCommand(com: String): Try[String] = {
    logger.debug(s"executing $com")
    Try(Seq("bash", "-c", com) !!)
  }

  def rsync(from: String, user: String, host: String, path: String, flags: String = "-Pvaz"): Try[String] = {
    _rsync(from, s"$user@$host:$path", flags)
  }

  private def _rsync(from: String, to: String, flags: String = "-Pvaz"): Try[String] = {
    Try(Seq("rsync", flags, from, to) !!)
  }

  def checkIfProcessRun(processName: String): Int = {
    executeCommand(s"ps aux | grep java | grep -v starter | grep -v grep | grep $processName | wc -l") match {
      case Success(str) => str.trim.toInt
      case Failure(err) => 0
    }
  }

}
