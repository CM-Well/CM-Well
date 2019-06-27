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
package cmwell.dc.stream.fingerprint

import akka.actor.{ActorSystem, Scheduler}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Framing}
import akka.util.ByteString
import cmwell.dc.Settings
import cmwell.dc.stream.MessagesTypesAndExceptions.{DcInfo, FingerPrintData, InfotonData, InfotonMeta}

import scala.concurrent.ExecutionContext

object FingerPrintFlow {

   def fingerprintFlow(dcInfo: DcInfo)
                      (implicit ec:ExecutionContext, scheduler:Scheduler, mat:ActorMaterializer, system:ActorSystem) = {
    Flow[InfotonData]
      .map(infoton => infoton.data)
      .via(Framing.delimiter(delimiter = ByteString("\n", "UTF-8"), 20000,
        allowTruncation = false))
      .map(_.utf8String)
      .filter(_.contains("<http://graph.link/ees/Uuid>"))
      .map(triple => triple.split(" ")(2).dropRight(1).drop(1))
      .scan(Set.empty[String])((total, element) => total.+(element))
      .mapConcat(identity)
       .mapAsync(Settings.fingerprintParallelism)(uuid => FingerPrintWebService.generateFingerPrint(fpUrl(dcInfo, uuid), getDestinationCluster(dcInfo)))
      .filter(_.nonEmpty)
      .map(rdf => InfotonData(InfotonMeta(rdf.utf8String.split(" ")(0)), rdf))
  }

  def fpUrl(dcInfo: DcInfo, uuid: String):String = {
    dcInfo.dcInfoExtra match {
      case Some(FingerPrintData(wsCluster, _)) => s"http://$wsCluster/user/$uuid"
      case _ => throw new IllegalArgumentException("Web service url for fingerprint was not provided")
    }
  }

  def getDestinationCluster(dcInfo: DcInfo):String = {
    dcInfo.dcInfoExtra match {
      case Some(FingerPrintData(_, dest)) => dest
      case _ => throw new IllegalArgumentException("Destination cluster for fingerprint was not provided")
    }
  }

}
