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
package cmwell.dc.stream.algo

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import cmwell.dc.stream.MessagesTypesAndExceptions.{BaseInfotonData, DcInfo, InfotonData}
import cmwell.dc.stream.Util
import cmwell.dc.{LazyLogging, stream}
import cmwell.util.loading.ChildFirstURLClassLoader
import ddpc.data.{AlgoFlow, IndentityFlow}

import scala.concurrent.ExecutionContext

object AlgoFlow extends LazyLogging{

  def algoFlow(dcInfo: DcInfo)
              (implicit ec:ExecutionContext,  mat:ActorMaterializer, system:ActorSystem) = {
    val algoFlow = Util.extractDcType(dcInfo.key.id) match {
      case "remote" =>
        new IndentityFlow().runAlgo(Map.empty[String, String])
      case _ =>
        val algoInfo = dcInfo.dcAlgoData.get
        val algoFlowInstance = ChildFirstURLClassLoader.loadClassFromJar[AlgoFlow](algoInfo.algoClass, algoInfo.algoJarUrl
          , "ddpc.data", Seq("scala", "akka", "org.slf4j"))
        algoFlowInstance.runAlgo(algoInfo.algoParams)
        }
    Flow[InfotonData]
      .map(_.base.data)
      .via(algoFlow)
      .map(rdf => BaseInfotonData(rdf.takeWhile(_ != stream.space).utf8String, rdf))

  }
}
//TODO:
//persist poisiton per bulk
