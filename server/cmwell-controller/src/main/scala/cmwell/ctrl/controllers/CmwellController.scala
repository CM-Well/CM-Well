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
import cmwell.ctrl.hc.ZookeeperUtils

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 2/16/15.
  */
object CmwellController {
  val components: Seq[ComponentController] = Seq(CassandraController,
                                                 ElasticsearchController,
                                                 KafkaController,
                                                 BgController,
                                                 WebserverController,
                                                 CwController,
                                                 DcController)

  def start {
    blocking {
      Future {
        components.foreach { c =>
          c.start; Thread.sleep(2000)
        }

        if (ZookeeperUtils.isZkNode)
          ZookeeperController.start
      }
    }
  }

  def stop {
    components.foreach(c => c.stop)
  }

  def clearData {
    components.foreach(c => c.clearData)
  }

}
