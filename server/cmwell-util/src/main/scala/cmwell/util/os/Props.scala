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
package cmwell.util.os

import java.lang.management.{ManagementFactory, RuntimeMXBean}
import cmwell.util.string._

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 7/23/13
  * Time: 10:37 AM
  * To change this template use File | Settings | File Templates.
  */
object Props {

  private[this] val runtimemxBean: RuntimeMXBean = ManagementFactory.getRuntimeMXBean
  private[this] val properties = runtimemxBean.getSystemProperties

  def getProcessUptime: Long = runtimemxBean.getUptime

  def getArgumentsForJVM: List[String] = {
    import scala.collection.JavaConverters._
    val arguments = runtimemxBean.getInputArguments
    arguments.asScala.toList
  }

  def printProps = {
    val it = runtimemxBean.getSystemProperties.keySet.iterator
    while (it.hasNext) {
      val next = it.next
      // scalastyle:off
      println(next + " -> " + runtimemxBean.getSystemProperties.get(next))
      // scalastyle:on
    }
  }

  val (pid, machineName) = {
    val pidAtName: String = ManagementFactory.getRuntimeMXBean.getName
    val arr = pidAtName.split('@')
    (arr(0), arr(1))
  }
  val os = java.lang.management.ManagementFactory.getOperatingSystemMXBean
    .asInstanceOf[com.sun.management.OperatingSystemMXBean]
  val isWin = properties.get("os.name").toLowerCase.contains("win")
  val pathWrapper = if (isWin) { (s: String) =>
    {
      if (s.exists(_ == ' ')) "\"" + s + "\""
      else s
    }
  } else identity[String](_) //same as: (s: String) => s
  val pathSeparator = properties.get("path.separator")
  val fileSeparator = properties.get("file.separator")
  val tempDirectory = properties.get("java.io.tmpdir")

  val tempDirectoryWithSeparator = tempDirectory.last match {
    case c if c == fileSeparator.head => tempDirectory
    case _                            => tempDirectory + fileSeparator
  }

  val osArchitecture = properties.get("sun.arch.data.model")
  val vmName = properties.get("java.vm.name")
  val (javaVersion, javaPatchNum) = ((s: String) => {
    val (t1, t2) = s.splitAt(s.indexOf('_'))
    (t1, t2.drop(1))
  })(properties.get("java.version"))
  val javaFullVersion = properties.get("java.version")
  val endln = properties.get("line.separator")
  val javaHome = pathWrapper(properties.get("java.home"))
  val javaApp = pathWrapper(properties.get("java.home") + fileSeparator + "bin" + fileSeparator + {
    if (isWin) "java.exe" else "java"
  })
  val jarName = unixify {
    System.getProperty("onejar.file") match {
      case null => {
        val rv = this.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
        if (isWin && (rv(0) == '\\' || rv(0) == '/')) rv.drop(1)
        else rv
      }
      case s: String => s
    }
  }
  lazy val machineIP: String = {
    import java.net._
    val interfaces = NetworkInterface.getNetworkInterfaces()
    val lb = scala.collection.mutable.ListBuffer[java.net.InetAddress]()
    while (interfaces.hasMoreElements) {
      val interface = interfaces.nextElement()
      if (interface.isUp) {
        val addresses = interface.getInetAddresses
        while (addresses.hasMoreElements) { lb.append(addresses.nextElement()) }
      }
    }
    lb.filter(_.isSiteLocalAddress).head.getHostAddress //TODO: Choose the ip address according to interface name.
  }

  def javaLib: String = {
    val jhr = javaHome.reverse.dropWhile(c => c == '\\' || c == '/')
    val jh = jhr.reverse
    val suffix = jhr.takeWhile(c => c != '\\' && c != '/').reverse
    if (suffix == "jre") jh.dropRight(3) + "lib"
    else if (suffix == "jre\"") jh.dropRight(4) + "\"lib"
    else if (suffix.matches("""jre\d+""")) jh.dropRight(suffix.length) + "lib"
    else if (suffix.matches("""jre\d+["]""")) jh.dropRight(suffix.length) + "\"lib"
    else
      javaHome + {
        val last = javaHome.reverse.head
        if (last == '\\' || last == '/') "lib"
        else fileSeparator + "lib"
      }
  }
}
