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
package cmwell.ctrl.commands

import cmwell.ctrl.controllers._

/**
  * Created by michael on 2/18/15.
  */
sealed trait ControlCommand {
  def execute
}

case object StartCassandra extends ControlCommand {
  override def execute: Unit = CassandraController.start
}

case object StopCassandra extends ControlCommand {
  override def execute: Unit = CassandraController.stop
}

case object StartElasticsearch extends ControlCommand {
  override def execute: Unit = ElasticsearchController.start
}

case object StopElasticsearch extends ControlCommand {
  override def execute: Unit = ElasticsearchController.stop
}

case object StartElasticsearchMaster extends ControlCommand {
  override def execute: Unit = ElasticsearchController.startMaster
}

case object StopElasticsearchMaster extends ControlCommand {
  override def execute: Unit = ElasticsearchController.stopMaster
}

case object StartBg extends ControlCommand {
  override def execute: Unit = BgController.start
}

case object StopBg extends ControlCommand {
  override def execute: Unit = BgController.stop
}

case object StartWebserver extends ControlCommand {
  override def execute: Unit = WebserverController.start
}

case object StopWebserver extends ControlCommand {
  override def execute: Unit = WebserverController.stop
}

case object RestartWebserver extends ControlCommand {
  override def execute: Unit = WebserverController.restart
}

case object StartCw extends ControlCommand {
  override def execute: Unit = CwController.start
}

case object StopCw extends ControlCommand {
  override def execute: Unit = CwController.stop
}

case object StartDc extends ControlCommand {
  override def execute: Unit = DcController.start
}

case object StopDc extends ControlCommand {
  override def execute: Unit = DcController.stop
}

case object StopKafka extends ControlCommand {
  override def execute: Unit = KafkaController.stop
}

case object StartKafka extends ControlCommand {
  override def execute: Unit = KafkaController.start
}
