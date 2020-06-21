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
package cmwell.ctrl.controllers

import cmwell.ctrl.config.Config
import cmwell.ctrl.utils.ProcUtil
import cmwell.util.http.{SimpleHttpClient => Http}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 2/16/15.
  */
object ElasticsearchController
    extends ComponentController(s"${Config.cmwellHome}/app/es/cur", "/log/es[0-9]*", Set("es")) {
  def stopElasticsearchRebalancing {
    //logger.info("[Elasticsearch controller] stopping ES rebalance")
    Http.put(s"http://${Config.pingIp}:9201/_cluster/settings",
             """{"transient" : {"cluster.routing.allocation.enable" : "none"}}""")
  }

  def startElasticsearchRebalancing {
    //logger.info("[Elasticsearch controller] resuming ES rebalance")
    Http.put(s"http://${Config.pingIp}:9201/_cluster/settings",
             """{"transient" : {"cluster.routing.allocation.enable" : "all"}}""")
  }

  def startMaster {
    ProcUtil.executeCommand(s"HAL=9000 ${super.getStartScriptLocation}/start-master.sh")
  }

  def stopMaster {
    ProcUtil.executeCommand(
      s"ps aux | grep es-master | grep -v grep | grep -v starter | awk '{print $$2}' | xargs kill"
    )
  }
}
