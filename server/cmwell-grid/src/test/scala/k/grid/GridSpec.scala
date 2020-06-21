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


package k.grid

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors

import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import k.grid.dmap.api._
import k.grid.dmap.impl.inmem.InMemDMap
import k.grid.dmap.impl.persistent.PersistentDMap
import k.grid.service.LocalServiceManager
import k.grid.testgrid.{WriteToPersistentDMap, DummyMessage}
import org.scalatest._

import scala.concurrent.duration._
import akka.pattern.ask

import scala.sys.process._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Created by markz on 5/14/14.
 */

object TestConfig {
  val config = ConfigFactory.load()
  val jarName = config.getString("grid.test.assembly-jar-name")
  val rootDir = config.getString("grid.test.root-dir")

}


class GridSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val timeout = Timeout(40.seconds)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))
  val expectedMembers = 4
  var serviceJvmName : String = _
  var serviceJvm : Option[GridJvm] = None


  def cleanup: Unit = {
    val filesToRemove = Seq("node-data", "client1-data", "client2-data", "sbt-tester-dmap").map(p => s"${TestConfig.rootDir}/$p/gridCluster")
    filesToRemove foreach {
      file =>
        if(Files.exists(Paths.get(file))) Files.delete(Paths.get(file))
    }
  }

  def spawnProcesses: Unit = {
    import scala.language.postfixOps
    // scalastyle:off
    Future{s"java -Dcmwell.grid.dmap.persistence.data-dir=${TestConfig.rootDir}/node-data -Dcmwell.grid.monitor.port=8001 -cp ${TestConfig.jarName} k.grid.testgrid.TestServiceNode" #> new File(s"${TestConfig.rootDir}/node.out") !}
    Future{s"java -Dcmwell.grid.dmap.persistence.data-dir=${TestConfig.rootDir}/client1-data -Dcmwell.grid.monitor.port=8002 -cp ${TestConfig.jarName} k.grid.testgrid.TestServiceClient" #> new File(s"${TestConfig.rootDir}/client1.out") !}
    Future{s"java -Dcmwell.grid.dmap.persistence.data-dir=${TestConfig.rootDir}/client2-data -Dcmwell.grid.monitor.port=8003 -cp ${TestConfig.jarName} k.grid.testgrid.TestServiceClient" #> new File(s"${TestConfig.rootDir}/client2.out") !}
    Thread.sleep(30000)
    // scalastyle:on
  }

  def killProcesses: Unit = {
    import scala.language.postfixOps
    Seq("ps", "aux") #| Seq("grep", "TestService") #| Seq("awk", "{print $2}") #| Seq("xargs", "kill", "-9") !
  }

  override protected def beforeAll() {
    cleanup

    spawnProcesses
    // create fs read
    super.beforeAll()

    Grid.setGridConnection(GridConnection(memberName = "tester",
      clusterName = "testgrid",
      hostName = "127.0.0.1",
      port = 0,
      seeds = Set("127.0.0.1:6666"),
      persistentDmapDir = "target/sbt-tester-dmap"
    ))



    Grid.joinClient

    Thread.sleep(40000)
  }

  override protected def afterAll() {
    super.afterAll()
    killProcesses
  }




  "member" should s"see all $expectedMembers grid members" in {
    Grid.jvmsAll.size should equal(expectedMembers)
  }

  "member" should "be able to send message to service actor and receive an answer" in {
    val txt = "Hello dummy service"
    val msg = DummyMessage(txt)

    val f = (Grid.serviceRef("DummyService") ? msg).mapTo[String]
    val res = Await.result(f, 30.seconds)
    res should equal(txt)
  }

  "member" should "be able to read InMemDMap values" in {
    val str = InMemDMap.get("some-key").get.asInstanceOf[SettingsString].str
    str should equal("some-value")
  }




  "service jvm" should "not be \"\"" in {
    serviceJvm = LocalServiceManager.getServiceJvm("DummyService")
    serviceJvmName = serviceJvm.map(_.jvmName).getOrElse("")
    serviceJvmName should not equal("")
  }

  "PersistentDMap" should "be persistent after jvm restarts" in {
    val m1 = Map(
      "os" -> SettingsString("WIN9.1"),
      "machine" -> SettingsString("MacBookPro"),
      "Suppliers" -> SettingsSet(Set("Apple","Microsoft","Ma'afe Ne'eman"))
    )

    val m2 = Map(
      "firstName" -> SettingsString("Almoni"),
      "profession" -> SettingsString("scala-ninja"),
      "previous-occupations" -> SettingsSet(Set("Pizza guy", "Astronaut", "Project Manager"))
    )

    Grid.serviceRef("DummyService") ! WriteToPersistentDMap(MapData(m1))

    Thread.sleep(10000)

    info("Data that was written should be seen in this node")
      PersistentDMap.sm should equal(m1)



    Grid.serviceRef("DummyService") ! WriteToPersistentDMap(MapData(m2), delay = 10.seconds)

    PersistentDMap.shutdownSlave

    Thread.sleep(15000)

    killProcesses
    Thread.sleep(5000)

    info("Data that was written while this node is down should not be written to this node")
      PersistentDMap.sm should equal(m1)


    spawnProcesses
    PersistentDMap.initSlave

    Thread.sleep(5000)

    info("After the JVM restarts all data should be written to this node")
      PersistentDMap.sm should equal(m1 ++ m2)


    PersistentDMap.set("longVal", SettingsLong(50L))
    Thread.sleep(5000)
    val res = PersistentDMap.get("longVal")
    res should equal(Some(SettingsLong(50L)))

  }


  "new service" should "be spawned on another jvm" in {
    Grid.selectActor(ClientActor.name, serviceJvm.get) ! RestartJvm
    Thread.sleep(60000)
    val newServiceJvm = LocalServiceManager.getServiceJvm("DummyService").map(_.jvmName).getOrElse("")
    newServiceJvm should not equal("")
    newServiceJvm should not equal(serviceJvmName)
  }



}
