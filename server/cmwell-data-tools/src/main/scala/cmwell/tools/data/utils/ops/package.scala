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
package cmwell.tools.data.utils
// scalastyle:off
import sun.misc.{Signal, SignalHandler}
// scalastyle:on
package object ops {

  /**
    * Response to SIGINFO event
    * @param handler handler to be fired when SIGINFO is received
    */
  def addSigInfoHook(handler: => Unit): Unit = {
    Signal.handle(
      new Signal("INFO"),
      new SignalHandler { override def handle(signal: Signal): Unit = handler }
    )
  }

  def getVersionFromManifest(): String = {
    getClass.getPackage.getSpecificationVersion
  }
}
