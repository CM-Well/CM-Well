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
package cmwell.dc.stream

import akka.util.ByteString

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by gilad on 1/4/16.
  */
object MessagesTypesAndExceptions {

    case class DcInfoKey(id: String, location: String, transformations: Map[String, String], ingestOperation: String, modifier: Option[String]) {

        override def toString: String =
          s"[id: $id, location: $location, transformations: ${transformations.mkString("(", ",", ")")}, modifier: ${modifier.fold("None")(identity)}]"
      }

      case class AlgoData(algoClass: String, algoJarUrl: String, algoParams: Map[String, String])

      case class DcInfo(key: DcInfoKey,
                        dcAlgoData: Option[AlgoData],
                        idxTime: Option[Long] = None,
                        positionKey: Option[String] = None,
                        tsvFile: Option[String] = None)

      case class WarmUpDcSync(dcInfo: DcInfo)

      case class StartDcSync(dcInfo: DcInfo)

      // A message to remove the sync from the sync map. Possible reasons:
      // 1. failed warm up (one of the futures in the warm up chain failed)
      // 2. the stream failed
      case class RemoveDcSync(dcInfo: DcInfo)

      case class SetDcSyncAsCancelled(dcInfoKey: DcInfoKey)

      case class StopDcSync(dcInfoKey: DcInfoKey)

      case class SaveDcSyncDoneInfo(dcInfo: DcInfo)

      case object CheckDcInfotonList

      case class RetrievedDcInfoList(dcInfoSeq: Seq[DcInfo])

      case class BaseInfotonData(path: String, data: ByteString) extends AnyRef

      case class InfotonData(base: BaseInfotonData, uuid: ByteString, indexTime: Long) extends AnyRef

      case class GetIndexTimeException(message: String, ex: Throwable = null) extends Exception(message, ex)

      case class GetInfotonListException(message: String, ex: Throwable = null) extends Exception(message, ex)

      case class RetrieveSyncInfotonsException(message: String, ex: Throwable = null) extends Exception(message, ex)

      //scalastyle:off
      case class ModifierMissingException(path: String) extends Exception(s"Modifier is missing for path: $path. Please add modifier to DC-Sync infoton or check your source environment.")
      case class UuidMissingException(path: String) extends Exception(s"Uuid is missing for path: $path")

  //scalastyle:on
      case class CreateConsumeException(message: String, ex: Throwable = null) extends Exception(message, ex)

      case class WrongPathGotException(message: String) extends Exception(message)

      case class RetrieveException(message: String, ex: Throwable = null) extends Exception(message, ex)

      case class RetrieveMissingUuidException(message: String, ex: Throwable = null) extends Exception(message, ex)

      case class RetrieveBadIndexTimeException(message: String, ex: Throwable = null) extends Exception(message, ex)

      case class RetrieveTsvException(message: String, ex: Throwable = null) extends Exception(message, ex)

      case class IngestException(message: String, ex: Throwable = null) extends Exception(message, ex)

      abstract class FuturedBodyException(val messageWithoutBody: String,
                                          val body: Future[String],
                                          val ex: Throwable = null)
        extends Exception(messageWithoutBody + " (complete error later with the same error id)", ex)

      case class IngestBadResponseException(override val messageWithoutBody: String,
                                            override val body: Future[String],
                                            override val ex: Throwable = null)
        extends FuturedBodyException(messageWithoutBody, body, ex)

      case class IngestServiceUnavailableException(override val messageWithoutBody: String,
                                                   override val body: Future[String],
                                                   override val ex: Throwable = null)
        extends FuturedBodyException(messageWithoutBody, body, ex)

      case class RetrieveBadResponseException(override val messageWithoutBody: String,
                                              override val body: Future[String],
                                              override val ex: Throwable = null)
        extends FuturedBodyException(messageWithoutBody, body, ex)

      case class RetrieveTsvBadResponseException(override val messageWithoutBody: String,
                                                 override val body: Future[String],
                                                 override val ex: Throwable = null)
        extends FuturedBodyException(messageWithoutBody, body, ex)


    }

