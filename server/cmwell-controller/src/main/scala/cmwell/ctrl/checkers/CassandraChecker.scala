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

import cmwell.ctrl.ddata.DData
import cmwell.ctrl.hc.HealthActor
import cmwell.ctrl.utils.ProcUtil
import cmwell.ctrl.config.Config._
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
  * Created by michael on 12/11/14.
  */
object CassandraChecker extends Checker with LazyLogging {
  override val storedStates: Int = 10

  private def getCassandraStatus(host: String = ""): ComponentState = {
    val pingIp = DData.getPingIp //HealthActor.getPingIp
    val com =
      if (host != "")
        s"$cmwellHome/conf/cas/cassandra-status-viewer $host"
      else if (pingIp != "")
        s"$cmwellHome/conf/cas/cassandra-status-viewer $pingIp"
      else
        s"$cmwellHome/conf/cas/cassandra-status-viewer"

    ProcUtil.executeCommand(com) match {
      case Success(res) =>
        val stats = res
          .split("\n")
          .map { r =>
            val t = r.split(" ")
            t(1) -> t(0)
          }
          .toMap

        val racks = res
          .split("\n")
          .map { r =>
            val t = r.split(" ")
            t(1) -> t(2)
          }
          .toMap
        val racksReversed = racks.groupBy { _._2 }.map { case (key, value) => (key, value.unzip._1) }
        CassandraOk(stats, racksReversed)
      case Failure(err) =>
        logger.error("Could not parse cassandra-status-viewer response", err)
        CassandraDown()
    }
  }

  override def check: Future[ComponentState] = {
    blocking {
      Future {
        getCassandraStatus()
      }
    }
  }

  def getReachableHost(hosts: Set[String]): Option[String] = {
    hosts.collectFirst {
      case host if (getCassandraStatus(host).isInstanceOf[CassandraOk]) => host
    }
  }
}
