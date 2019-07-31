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
package cmwell.dc.stream.algo

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import cmwell.dc.LazyLogging
import cmwell.dc.stream.MessagesTypesAndExceptions.{DcInfo, InfotonData, InfotonThinMeta}
import cmwell.util.loading.URLClassLoader
import ddpc.data.{AlgoFlow, IndentityFlow}

import scala.concurrent.ExecutionContext

object AlgoFlow extends LazyLogging{

  def algoFlow(dcInfo: DcInfo)
              (implicit ec:ExecutionContext,  mat:ActorMaterializer, system:ActorSystem) = {
    logger.info("HI lala, in algoFlow")

    val algoFlow = dcInfo.dcInfoExtraType match {
      case "remote" =>
        new IndentityFlow().runAlgo(Map.empty[String, String])
      case _ =>
        val algoInfo = dcInfo.dcInfoExtra.get
        val algoFlowInstance = URLClassLoader.loadClassFromJar[AlgoFlow](algoInfo.algoClass, algoInfo.algoJarUrl)
        algoFlowInstance.runAlgo(algoInfo.algoParams)
        }
    Flow[InfotonData]
      .map(_.data)
      .via(algoFlow)
      .map(rdf => InfotonData(InfotonThinMeta(rdf.utf8String.split(" ")(0)), rdf))
  }
}


//TODO:

//smart key
//move index time from ingest to _out
//zstore
//persist poisiton per bulk
//load application.conf

