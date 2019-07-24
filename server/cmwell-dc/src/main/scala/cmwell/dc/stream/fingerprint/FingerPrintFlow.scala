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
import akka.event.Logging
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.scaladsl.{Flow, Framing}
import akka.util.ByteString
import cmwell.dc.Settings
import cmwell.dc.stream.MessagesTypesAndExceptions.{DcInfo, FingerPrintData, InfotonData, InfotonFullMeta}

import scala.concurrent.{ExecutionContext, Future}

object FingerPrintFlow {

   def fingerprintFlow(dcInfo: DcInfo)
                      (implicit ec:ExecutionContext,  mat:ActorMaterializer, system:ActorSystem) = {
    Flow[InfotonData]
      .map(_.data)
      .via(Framing.delimiter(delimiter = ByteString("\n", "UTF-8"), Settings.maxEventFrameSize,
        allowTruncation = false))
      .map(_.utf8String)
      .filter(_.contains("<http://graph.link/ees/Uuid>"))
      .map(triple => triple.split(" ")(2).dropRight(1).drop(1))
      .statefulMapConcat{ () =>
        var uuidsSet = Set.empty[String]
        uuid =>
          if (uuidsSet.contains(uuid)) {
            List("")
          }
         else {
           uuidsSet = uuidsSet + uuid
           List(uuid)
         }
      }
       .filter(_.nonEmpty)
      .log("afterScan")
      .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel, onFinish = Logging.InfoLevel, onFailure = Logging.InfoLevel))
       .mapAsync(Settings.fingerprintParallelism)(uuid => fpUrl(dcInfo, uuid).flatMap(url => FingerPrintWebService.generateFingerPrint(url)))
       .filter(_.nonEmpty)
      .map(rdf => InfotonData(InfotonFullMeta(rdf.utf8String.split(" ")(0), ByteString("no-uuid", "UTF-8"), -1), rdf))
  }

  def fpUrl(dcInfo: DcInfo, uuid: String)(implicit ec:ExecutionContext):Future[String] = {
    dcInfo.dcInfoExtra match {
      case Some(FingerPrintData(wsCluster)) => Future.successful(s"http://$wsCluster/user/$uuid")
      case _ => Future.failed(new IllegalArgumentException("Web service url for fingerprint was not provided"))
    }
  }

}
