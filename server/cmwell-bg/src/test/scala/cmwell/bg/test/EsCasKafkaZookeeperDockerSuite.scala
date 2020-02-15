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

package cmwell.bg.test

import cmwell.util.testSuitHelpers.test.EsCasKafkaZookeeperDockerSuite
import org.scalatest.Suite

trait BgEsCasKafkaZookeeperDockerSuite extends EsCasKafkaZookeeperDockerSuite { this:Suite =>
  override def elasticsearchVersion: String = cmwell.util.build.BuildInfo.elasticsearchVersion
  override def cassandraVersion: String = cmwell.util.build.BuildInfo.cassandraVersion
  //the below should be restored when Kafka image of Scala 2.13 will be available
  //override def kafkaVersion: String = s"${cmwell.util.build.BuildInfo.scalaVersion.take(4)}-${cmwell.util.build.BuildInfo.kafkaVersion}"
  override def kafkaVersion: String = s"2.12-${cmwell.util.build.BuildInfo.kafkaVersion}"
  override def zookeeperVersion: String = cmwell.util.build.BuildInfo.zookeeperVersion
}
