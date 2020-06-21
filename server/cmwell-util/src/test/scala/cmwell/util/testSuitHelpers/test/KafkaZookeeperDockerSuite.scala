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

package cmwell.util.testSuitHelpers.test

import com.dimafeng.testcontainers.ForAllTestContainer
import org.scalatest.Suite

trait KafkaZookeeperDockerSuite extends ForAllTestContainer { this: Suite =>
  def zookeeperVersion: String
  def kafkaVersion: String
  val KafkaZookeeperContainers(kafkaContainer, zookeeperContainer, combined) = ContainerHelpers.kafkaAndZookeeper(kafkaVersion, zookeeperVersion)
  override val container = combined

  override def afterStart(): Unit = {
    super.afterStart()
    // scalastyle:off
    kafkaContainer.configure{ container =>
      val result = container.execInContainer("bash", "-c", "${KAFKA_HOME}/bin/kafka-configs.sh " +
        "--bootstrap-server localhost:19092 --entity-type brokers --entity-name 1 --alter --add-config " +
        s"advertised.listeners=[EXTERNAL://${kafkaContainer.containerIpAddress}:${kafkaContainer.mappedPort(9092)},INTERNAL://kafkaBroker-1:19092]")
      val stdOut = result.getStdout.trim
      if (stdOut != "Completed updating config for broker: 1.") {
        val stdErr = result.getStderr.trim
        throw new Exception(s"Couldn't change Kafka's advertised listeners config for broker 1. stdout: [$stdOut]. stderr: [$stdErr]")
      }
    }
    // scalastyle:on
  }
}
