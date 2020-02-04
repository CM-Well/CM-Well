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

package cmwell.build

import java.util.Locale

import cmwell.build.OSType.OSType

/**
  * helper class to check the operating system this Java VM runs in
  *
  * please keep the notes below as a pseudo-license
  *
  * http://stackoverflow.com/questions/228477/how-do-i-programmatically-determine-operating-system-in-java
  * compare to http://svn.terracotta.org/svn/tc/dso/tags/2.6.4/code/base/common/src/com/tc/util/runtime/Os.java
  * http://www.docjar.com/html/api/org/apache/commons/lang/SystemUtils.java.html
  */

object OSType extends Enumeration {
  type OSType = Value
  val Windows, MacOS, Linux, Other = Value
}

object OsCheck {

  /**
    * types of Operating Systems
    */

  // cached result of OS detection
  protected var detectedOS: OSType = null

  /**
    * detect the operating system from the os.name System property and cache
    * the result
    *
    * @returns - the operating system detected
    */
  def getOperatingSystemType: OSType = {
    if (detectedOS == null) {
      val OS = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH)
      if (OS.contains("mac") || OS.contains("darwin")) detectedOS = OSType.MacOS
      else if (OS.contains("win")) detectedOS = OSType.Windows
      else if (OS.contains("nux")) detectedOS = OSType.Linux
      else detectedOS = OSType.Other
    }
    detectedOS
  }
}
