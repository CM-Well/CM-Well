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

import javax.management.ObjectName
import java.lang.management.ManagementFactory
import scala.language.implicitConversions

/**
  * User: israel
  * Date: 12/11/13
  * Time: 20:56
  */
package object jmx {
  implicit def string2objectName(name: String): ObjectName = new ObjectName(name)

  def jmxRegister(ob: Object, obname: ObjectName): Unit = {
    val mServer = ManagementFactory.getPlatformMBeanServer

    if (!mServer.isRegistered(obname)) {
      mServer.registerMBean(ob, obname)
    }
  }
  def jmxRegister(ob: Object): Unit = {
    val objFullName = this.getClass.getCanonicalName
    val (packageName, className) = objFullName.lastIndexOf('.') match {
      case i if i > 0 => (objFullName.take(i), objFullName.takeRight(objFullName.length - (i + 1)))
      case _          => (objFullName, objFullName)
    }
    val oName = new ObjectName(s"$packageName:type=$className")
    jmxRegister(ob, oName)

  }

  def jmxUnRegister(obname: ObjectName) = ManagementFactory.getPlatformMBeanServer.unregisterMBean(obname)
}
