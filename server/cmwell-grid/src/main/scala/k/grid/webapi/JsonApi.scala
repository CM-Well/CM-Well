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
package k.grid.webapi

import k.grid.webapi.jsobj.{GridMemoryInfo, MachineMemoryInfo}
import k.grid.{Grid, GridJvm}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by michael on 3/20/16.
  */
object JsonApi {
  def getJvmsJson: String = {
    def genChildren(children: Set[GridJvm]): String = {
      children
        .map { child =>
          s"""
             |{
             |  "name":"${child.identity.map(_.name).getOrElse("NA")}",
             |  "size": 10
             |}
           """.stripMargin
        }
        .mkString(",")
    }

    val hostJvms = Grid.jvmsAll.groupBy(_.host)

    val body = hostJvms
      .map { host =>
        s"""
           |{
           |  "name":"${host._1}",
           |  "children": [${genChildren(host._2)}]
           |}
         """.stripMargin
      }
      .mkString(",")

    s"""
       |{
       |  "name": "${Grid.clusterName}",
       |  "children": [$body]
       |}
     """.stripMargin
  }

  def getMemJson: Future[String] = {
    Grid.getAllJvmsInfo.map { m =>
      val clusterName = Grid.clusterName
      val sampleTime = System.currentTimeMillis()

      val machinesUsages = m.map { tup =>
        MachineMemoryInfo(tup._1.host,
                          tup._1.identity.map(_.name).getOrElse(""),
                          tup._2.memInfo.map(mi => mi.name -> mi.usedPercent.toLong).toMap)
      }.toSet

      Json.prettyPrint(GridMemoryInfo(clusterName, sampleTime, machinesUsages).toJson)
    }
  }
}
