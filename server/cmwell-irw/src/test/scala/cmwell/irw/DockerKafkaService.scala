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
package cmwell.irw

//import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}
//
//trait DockerKafkaService extends DockerKit {
//
//  def KafkaAdvertisedPort = 9092
//  val ZookeeperDefaultPort = 2181
//
//  lazy val kafkaContainer = DockerContainer("spotify/kafka")
//    .withPorts(KafkaAdvertisedPort -> Some(KafkaAdvertisedPort), ZookeeperDefaultPort -> None)
//    .withEnv(s"ADVERTISED_PORT=$KafkaAdvertisedPort", s"ADVERTISED_HOST=${dockerExecutor.host}")
//    .withReadyChecker(DockerReadyChecker.LogLineContains("kafka entered RUNNING state"))
//
//  abstract override def dockerContainers: List[DockerContainer] =
//    kafkaContainer :: super.dockerContainers
//}
