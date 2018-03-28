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
package cmwell.stortill

import akka.actor.ActorSystem
import cmwell.driver.Dao
import cmwell.fts.FTSServiceES
import cmwell.irw.IRWService

import scala.io.Source

/**
  * Created by markz on 3/26/15.
  */
object DupClener extends App {
  val hostname = args(0)
  val clusterName = args(1)
  val pathsFile = args(2)
  val numOfVersions = args(3).toInt

  // scalastyle:off
  println(s" ${hostname}  ${clusterName} ${pathsFile} ")
  // scalastyle:on
  // init dao & irw & fts
  System.setProperty("ftsService.transportAddress", hostname)
  System.setProperty("ftsService.clusterName", clusterName)
  val dao = Dao("operation", "data", hostname, 10)
  val irw = IRWService(dao)
  val fts = FTSServiceES.getOne("ftsService.yml")

  val f = ProxyOperations(irw, fts)

  for (line <- Source.fromFile(pathsFile).getLines()) {
    // scalastyle:off
    println(s"Working on $line")
    // scalastyle:on
    f.fix(line, 1, numOfVersions)
  }
  // scalastyle:off
  println("Done.")
  println("Shutdown tool.")
  // scalastyle:on
  f.shutdown
}
