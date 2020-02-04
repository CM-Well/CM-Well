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
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

/**
  * Created by michael on 10/5/14.
  */
case class PortManager(initialPort: Int, step: Int) {
  def getPort(index: Int): Int = {
    initialPort + step * (index - 1)
  }
}

case object CassandraPortManager {
  val jmxPortManager = PortManager(7199, 2)
  val dmx4jPortManager = PortManager(8989, 1)
}

case object ElasticsearchPortManager {
  val jmxPortManager = PortManager(7200, 2)
  val httpPortManager = PortManager(9201, 1)
  val transportPortManager = PortManager(9301, 1)
}

case object BgPortManager {
  val jmxPortManager = PortManager(7196, 1)
  val monitorPortManager = PortManager(8050, 1)
}

case object CtrlPortManager {
  val monitorPortManager = PortManager(8000, 1)
  val jmxPortManager = PortManager(7192, 1)
}

case object WebServicePortManager {
  val jmxPortManager = PortManager(7194, 1)
  val playHttpPortManager = PortManager(9000, 1)
  val monitorPortManager = PortManager(8010, 1)
}

case object CwPortManager {
  val monitorPortManager = PortManager(8030, 1)
}

case object DcPortManager {
  val monitorPortManager = PortManager(8040, 1)
  val jmxPortManager = PortManager(7193, 1)
}

case object KafkaPortManager {
  val jmxPortManager = PortManager(7191, 1)
}

object PortManagers {
  val cas = CassandraPortManager
  val es = ElasticsearchPortManager
  val bg = BgPortManager
  val ws = WebServicePortManager
  val ctrl = CtrlPortManager
  val cw = CwPortManager
  val dc = DcPortManager
  val kafka = KafkaPortManager
}
