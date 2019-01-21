///**
//  * Copyright 2015 Thomson Reuters
//  *
//  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *   http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
//  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  *
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
//package cmwell.bg.test
//
//import cmwell.util.build.BuildInfo.elasticsearchVersion
//import com.typesafe.config.ConfigFactory
//import org.scalatest._
//import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}
//
//import scala.io.Source
//
//object EmbeddedES {
//  System.setProperty("es.set.netty.runtime.available.processors", "false")
//
//  lazy val config = ConfigFactory.load()
//  lazy val esClusterName = config.getString("ftsService.clusterName")
//  lazy val indicesTemplate = Source.fromURL(this.getClass.getResource("/indices_template.json")).getLines.reduceLeft(_ + _)
//  print(s"indices template: $indicesTemplate")
//  print("\n")
//  lazy val embeddedElastic:EmbeddedElastic = EmbeddedElastic.builder()
//    .withElasticVersion(elasticsearchVersion)
//    .withSetting(PopularProperties.CLUSTER_NAME, esClusterName)
//    .withTemplate("indices_template", indicesTemplate)
//    .withIndex("cm_well_p0_0")
//    .build()
//}
//
//trait EmbeddedESSuite extends Suite with BeforeAndAfterAll { this: Suite =>
//
//
//  override def beforeAll(): Unit = {
//    if(nestedSuites.nonEmpty) {
//      print("Starting EmbeddedElasticSearch before all test suites")
//      print("\n")
//      EmbeddedES.embeddedElastic.start()
//    } else {
//      EmbeddedES.embeddedElastic.recreateIndices()
//    }
//    super.beforeAll()
//  }
//
//  override def afterAll(): Unit = {
//    if(nestedSuites.nonEmpty) {
//      print("Stopping EmbeddedElasticSearch")
//      print("\n")
//      EmbeddedES.embeddedElastic.stop()
//    }
//    super.afterAll()
//  }
//}
//
//class BGSpecs extends Suites(
//  new BGMergerSpec,
//  new BGSeqSpecs
//)
//
//@DoNotDiscover
//class BGSeqSpecs extends Suites (
//  new CmwellBGSpec with EmbeddedESSuite,
//  new BGResilienceSpec with EmbeddedESSuite,
//  new BGSequentialSpec with EmbeddedESSuite
//) with SequentialNestedSuiteExecution with EmbeddedESSuite
