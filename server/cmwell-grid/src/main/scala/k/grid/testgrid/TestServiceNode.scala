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
package k.grid.testgrid

import akka.util.Timeout
import k.grid.dmap.api.SettingsString
import k.grid.dmap.impl.inmem.InMemDMap
import k.grid.dmap.impl.persistent.PersistentDMap
import k.grid.service.ServiceTypes

import k.grid.{Grid, GridConnection}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 4/20/16.
  */
object TestServiceNode extends App {
  implicit val timeout = Timeout(3.seconds)
  Grid.setGridConnection(
    GridConnection(memberName = "ctrl",
                   clusterName = "testgrid",
                   hostName = "127.0.0.1",
                   port = 6666,
                   seeds = Set("127.0.0.1:6666"))
  )
  Grid.join

}

object TestServiceClient extends App {
  implicit val timeout = Timeout(3.seconds)

  val randomName = s"client${System.currentTimeMillis()}"

  Grid.setGridConnection(
    GridConnection(memberName = randomName,
                   clusterName = "testgrid",
                   hostName = "127.0.0.1",
                   port = 0,
                   seeds = Set("127.0.0.1:6666"))
  )

  Grid.declareServices(ServiceTypes().add("DummyService", classOf[DummyService]))

  Grid.joinClient

  Grid.system.scheduler.scheduleOnce(10.seconds) {
    InMemDMap.set("some-key", SettingsString("some-value"))
  }
}
