/**
  * Copyright 2015 Thomson Reuters
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

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer, MultipleContainers}
import org.scalatest.Suite
import org.slf4j.LoggerFactory
import org.testcontainers.Testcontainers
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.Base58

trait KafkaZookeeperDockerSuite extends ForAllTestContainer { this:Suite =>
  def zookeeperVersion: String
  def kafkaVersion: String
  val internalNetwork = Network.newNetwork()

  lazy val zookeeperContainer = {
    val scalaContainer = GenericContainer(s"zookeeper:$zookeeperVersion",
      exposedPorts = Seq(2181),
      waitStrategy = Wait.forLogMessage(".*binding to port 0.0.0.0/0.0.0.0:2181.*\n", 1)
    )
    scalaContainer.configure{ container =>
      container.withNetwork(internalNetwork)
      container.withNetworkAliases("zookeeper")
    }
    scalaContainer
  }
  lazy val kafkaContainer = {
    val scalaContainer = GenericContainer(s"wurstmeister/kafka:$kafkaVersion",
      exposedPorts = Seq(9092),
      env = Map(
        "KAFKA_ZOOKEEPER_CONNECT" -> s"zookeeper:2181",
        "KAFKA_LISTENERS" -> s"PLAINTEXT://:9092",
//        "KAFKA_CREATE_TOPICS" -> s"Topic1:1:1",
      ),
      waitStrategy = Wait.forLogMessage(".*KafkaServer.*started.*\n", 1)
    )
    scalaContainer.configure{ container =>
      container.withNetwork(internalNetwork)
      val networkAlias = "kafkaBroker-" + Base58.randomString(6)
      container.withNetworkAliases(networkAlias)
      //The network alias can be used the advertising listeners later (for multi brokers configuration)
      container.addEnv("KAFKA_ADVERTISED_LISTENERS", s"PLAINTEXT://$networkAlias:9092")
    }
    scalaContainer
  }

  override val container = MultipleContainers(zookeeperContainer, kafkaContainer)

  override def afterStart() {
    val zookeeperLogger = new Slf4jLogConsumer(LoggerFactory.getLogger(s"${zookeeperContainer.containerInfo.getConfig.getImage}"))
    zookeeperContainer.configure(j => j.followOutput(zookeeperLogger))
    val kafkaLogger = new Slf4jLogConsumer(LoggerFactory.getLogger(s"${kafkaContainer.containerInfo.getConfig.getImage}"))
    kafkaContainer.configure(j => j.followOutput(kafkaLogger))
  }
}
