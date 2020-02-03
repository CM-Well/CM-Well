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
package cmwell.ctrl.checkers

import cmwell.ctrl.utils.ProcUtil
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.Future
import java.net._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * Created by michael on 1/5/15.
  */
case class DiskUsage(name: String, usage: Float)
object SystemChecker extends Checker with LazyLogging {

  private def deviceUsage: Set[DiskUsage] = {
    val resTry =
      ProcUtil.executeCommand("""df -h | awk '{print $5 " " $6}' | awk -F "% " '{print $1 " " $2}' | tail -n+2""")
    resTry match {
      case Success(res) =>
        res.trim
          .split("\n")
          .map { t =>
            val vals = t.split(" ")
            DiskUsage(vals(1), vals(0).toFloat)
          }
          .toSet
      case Failure(err) =>
        logger.error("Couldn't retrieve disk usage", err)
        Set.empty[DiskUsage]
    }
  }

  override val storedStates: Int = 10
  override def check: Future[ComponentState] = {
//    val usages = deviceUsage
//    usages.foreach {
//      usage =>
//        val name = usage.name
//        val usagePercent = usage.usage
//        logger.info(s"DiskUsage: $name $usagePercent")
//    }

    val interfaces = NetworkInterface.getNetworkInterfaces.asScala
      .flatMap(_.getInetAddresses.asScala.toList)
      .filter(addr => addr != null && addr.getHostAddress.matches("""\d+.\d+.\d+.\d+"""))
      .toVector
    val name = InetAddress.getLocalHost().getHostName().split('.')(0)
    Future.successful(SystemResponse(interfaces.map(inet => inet.getHostAddress), name))
  }
}
