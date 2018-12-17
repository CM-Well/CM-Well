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

/*
import cmwell.irw.DockerKafkaService
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.time.{Second, Seconds, Span}
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava

class KafkaServiceSpec
  extends FlatSpec
    with Matchers
    with DockerKafkaService
    with DockerTestKit
    with DockerKitDockerJava {

  implicit val pc = PatienceConfig(Span(20, Seconds), Span(1, Second))

  "kafka container" should "be ready" in {
    isContainerReady(kafkaContainer).futureValue shouldBe true
  }

}
*/
