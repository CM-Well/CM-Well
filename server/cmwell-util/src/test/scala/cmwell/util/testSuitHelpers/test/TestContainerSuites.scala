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

import org.scalatest.{DoNotDiscover, FlatSpec}

//All tests here are annotated @DoNotDiscover so they won't run during normal tests.

@DoNotDiscover
class TestKZSuite extends FlatSpec with KafkaZookeeperDockerSuite {
  override def zookeeperVersion: String = "3.4.13"
  override def kafkaVersion: String = "2.12-2.1.0"

  "KafkaZookeeperDockerSuite" should "work" in {
    //dummy line for breakpoint
    val a = 10
    assert(true)
  }

}

@DoNotDiscover
class TestESSuite extends FlatSpec with ElasticsearchDockerSuite {
  override def elasticsearchVersion: String = "6.5.4"

  "ElasticsearchDockerSuite" should "work" in {
    assert(true)
  }
}

@DoNotDiscover
class TestCasSuite extends FlatSpec with CassandraDockerSuite {
  override def cassandraVersion: String = "3.11.3"

  "ElasticsearchDockerSuite" should "work" in {
    assert(true)
  }

}

@DoNotDiscover
class TestESKafZooSuite extends FlatSpec with EsKafkaZookeeperDockerSuite {
  override def elasticsearchVersion: String = "6.5.4"
  override def zookeeperVersion: String = "3.4.13"
  override def kafkaVersion: String = "2.12-2.1.0"

  "EsKafkaZookeeperDockerSuite" should "work" in {
    assert(true)
  }
}

@DoNotDiscover
class TestESCasKafZooSuite extends FlatSpec with EsCasKafkaZookeeperDockerSuite {
  override def elasticsearchVersion: String = "6.5.4"
  override def zookeeperVersion: String = "3.4.13"
  override def kafkaVersion: String = "2.12-2.1.0"
  override def cassandraVersion: String = "3.11.3"

  "EsCasKafkaZookeeperDockerSuite" should "work" in {
    assert(true)
  }
}
