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
import com.dimafeng.testcontainers.{Container, GenericContainer, MultipleContainers}
import org.slf4j.LoggerFactory
import org.testcontainers.containers.Network
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait

case class KafkaZookeeperContainers(kafkaContainer: GenericContainer, zookeeperContainer: GenericContainer, combined: Container)

object ContainerHelpers {

  def kafkaAndZookeeper(kafkaVersion: String, zookeeperVersion: String): KafkaZookeeperContainers = {
    val internalNetwork = Network.newNetwork()
    lazy val zookeeperContainer = {
      val scalaContainer = GenericContainer(s"zookeeper:$zookeeperVersion",
        exposedPorts = Seq(2181),
        waitStrategy = Wait.forLogMessage(".*binding to port /0.0.0.0:2181.*\n", 1)
      )
      scalaContainer.configure { container =>
        container.withNetwork(internalNetwork)
        container.withNetworkAliases("zookeeper")
        val logger = new Slf4jLogConsumer(LoggerFactory.getLogger(container.getDockerImageName))
        container.withLogConsumer(logger)
      }
      scalaContainer
    }
    lazy val kafkaContainer = {
      val externalPort = 9092
      val internalPort = 10000 + externalPort
      val brokerId = 1
      val scalaContainer = GenericContainer(s"wurstmeister/kafka:$kafkaVersion",
        exposedPorts = Seq(externalPort),
        env = Map(
          "KAFKA_ZOOKEEPER_CONNECT" -> s"zookeeper:2181",
          "KAFKA_LISTENERS" -> s"INTERNAL://0.0.0.0:$internalPort,EXTERNAL://0.0.0.0:$externalPort",
          "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" -> s"INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
          "KAFKA_INTER_BROKER_LISTENER_NAME" -> s"INTERNAL",
          "KAFKA_CREATE_TOPICS" -> s"persist_topic:1:1,persist_topic.priority:1:1,index_topic:1:1,index_topic.priority:1:1",
          "KAFKA_BROKER_ID" -> s"$brokerId"
        ),
        waitStrategy = Wait.forLogMessage(".*KafkaServer.*started.*\n", 1)
      )
      scalaContainer.configure { container =>
        container.withNetwork(internalNetwork)
        val networkAlias = s"kafkaBroker-$brokerId"
        container.withNetworkAliases(networkAlias)
        //The network alias can be used the advertising listeners later (for multi brokers configuration)
        container.addEnv("KAFKA_ADVERTISED_LISTENERS", s"INTERNAL://$networkAlias:$internalPort")
        val logger = new Slf4jLogConsumer(LoggerFactory.getLogger(container.getDockerImageName))
        container.withLogConsumer(logger)
      }
      scalaContainer
    }
    val combined = MultipleContainers(zookeeperContainer, kafkaContainer)
    KafkaZookeeperContainers(kafkaContainer, zookeeperContainer, combined)
  }

  def elasticsearch(elasticsearchVersion: String): GenericContainer = {
    val scalaContainer = GenericContainer(s"docker.elastic.co/elasticsearch/elasticsearch-oss:$elasticsearchVersion",
      exposedPorts = Seq(9200),
      waitStrategy = Wait.forHttp("/").forPort(9200).forStatusCode(200),
      env = Map("discovery.type" -> "single-node", "ES_JAVA_OPTS" -> "-Xms2000m -Xmx2000m")
    )
    scalaContainer.configure { container =>
      val logger = new Slf4jLogConsumer(LoggerFactory.getLogger(s"elasticsearch-oss:$elasticsearchVersion"))
      container.withLogConsumer(logger)
    }
    scalaContainer
  }

  def cassandra(cassandraVersion: String): GenericContainer = {
    val scalaContainer = GenericContainer(s"cassandra:$cassandraVersion",
      waitStrategy = Wait.forLogMessage(".*Starting listening for CQL clients.*\n", 1),
      env = Map("JVM_OPTS" -> "-Xms1G -Xmx1G")
    )
    scalaContainer.configure { container =>
      val logger = new Slf4jLogConsumer(LoggerFactory.getLogger(container.getDockerImageName))
      container.withLogConsumer(logger)
    }
    scalaContainer
  }
}
