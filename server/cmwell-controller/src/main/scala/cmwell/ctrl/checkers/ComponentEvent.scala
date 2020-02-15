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

/**
  * Created by michael on 2/19/15.
  */
trait ComponentEvent {
  def id: String
}

trait WebEvent extends ComponentEvent
case class WebNormalEvent(id: String) extends WebEvent
case class WebBadCodeEvent(id: String, code: Int) extends WebEvent
case class WebDownEvent(id: String) extends WebEvent

trait BgEvent extends ComponentEvent
case class BgOkEvent(id: String) extends BgEvent
case class BgNotOkEvent(id: String) extends BgEvent

trait CassandraEvent extends ComponentEvent
case class CassandraNodeNormalEvent(id: String) extends CassandraEvent
case class CassandraNodeDownEvent(id: String) extends CassandraEvent
case class CassandraNodeUnavailable(id: String) extends CassandraEvent

trait ElasticsearchEvent extends ComponentEvent
case class ElasticsearchNodeGreenEvent(id: String) extends CassandraEvent
case class ElasticsearchNodeYellowEvent(id: String) extends CassandraEvent
case class ElasticsearchNodeRedEvent(id: String) extends CassandraEvent
case class ElasticsearchNodeDownEvent(id: String) extends CassandraEvent
case class ElasticsearchNodeBadCodeEvent(id: String, code: Int) extends CassandraEvent

trait DcEvent extends ComponentEvent
case class DcNormalEvent(id: String, checkerHost: String) extends DcEvent
case class DcNotSyncingEvent(id: String, checkerHost: String) extends DcEvent
case class DcLostConnectionEvent(id: String, checkerHost: String) extends DcEvent
case class DcCouldNotGetDcEvent(id: String, checkerHost: String) extends DcEvent
case class DcCouldNotGetDcLongEvent(id: String, checkerHost: String) extends DcEvent
case class DcCouldNotGetDataEvent(checkerHost: String) extends DcEvent {
  val id = "na"
}

trait ZookeeperEvent extends ComponentEvent
case class ZkOkEvent(id: String) extends ZookeeperEvent
case class ZkReadOnlyEvent(id: String) extends ZookeeperEvent
case class ZkNotOkEvent(id: String) extends ZookeeperEvent
case class ZkNotRunningEvent(id: String) extends ZookeeperEvent
case class ZkSeedNotRunningEvent(id: String) extends ZookeeperEvent

trait KafkaEvent extends ComponentEvent
case class KafkaOkEvent(id: String) extends KafkaEvent
case class KafkaNotOkEvent(id: String) extends KafkaEvent

trait SystemEvent extends ComponentEvent

case class ReportTimeoutEvent(id: String) extends ComponentEvent

case class ComponentChangedColorEvent(id: String, color: StatusColor) extends ComponentEvent
