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
package cmwell.dc.stream

import akka.util.ByteString

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by gilad on 1/4/16.
  */
object MessagesTypesAndExceptions {

  case class DcInfo(id: String,
                    location: String,
                    idxTime: Option[Long] = None,
                    positionKey: Option[String] = None,
                    tsvFile: Option[String] = None)

  case class WarmUpDcSync(dcInfo: DcInfo)
  case class StartDcSync(dcInfo: DcInfo)
  // A message to remove the sync from the sync map. Possible reasons:
  // 1. failed warm up (one of the futures in the warm up chain failed)
  // 2. the stream failed
  case class RemoveDcSync(dcInfo: DcInfo)
  case class SetDcSyncAsCancelled(dcInfo: DcInfo)
  case class StopDcSync(dcInfo: DcInfo)
  case class SaveDcSyncDoneInfo(dcInfo: DcInfo)
  case object CheckDcInfotonList
  case class RetrievedDcInfoList(dcInfoSeq: Seq[DcInfo])

  case class InfotonMeta(path: String, uuid: ByteString, indexTime: Long)
  case class InfotonData(meta: InfotonMeta, data: ByteString)

  case class GetIndexTimeException(message: String, ex: Throwable = null)
      extends Exception(message, ex)
  case class GetInfotonListException(message: String, ex: Throwable = null)
      extends Exception(message, ex)
  case class RetrieveSyncInfotonsException(message: String,
                                           ex: Throwable = null)
      extends Exception(message, ex)
  case class CreateConsumeException(message: String, ex: Throwable = null)
      extends Exception(message, ex)
  case class WrongPathGotException(message: String) extends Exception(message)
  case class RetrieveException(message: String, ex: Throwable = null)
      extends Exception(message, ex)
  case class RetrieveMissingUuidException(message: String, ex: Throwable = null)
      extends Exception(message, ex)
  case class RetrieveBadIndexTimeException(message: String,
                                           ex: Throwable = null)
      extends Exception(message, ex)
  case class RetrieveTsvException(message: String, ex: Throwable = null)
      extends Exception(message, ex)
  case class IngestException(message: String, ex: Throwable = null)
      extends Exception(message, ex)

  abstract class FuturedBodyException(val messageWithoutBody: String,
                                      val body: Future[String],
                                      val ex: Throwable = null)
      extends Exception(
        messageWithoutBody + " (complete error later with the same error id)",
        ex
      )
  case class IngestBadResponseException(override val messageWithoutBody: String,
                                        override val body: Future[String],
                                        override val ex: Throwable = null)
      extends FuturedBodyException(messageWithoutBody, body, ex)
  case class IngestServiceUnavailableException(
    override val messageWithoutBody: String,
    override val body: Future[String],
    override val ex: Throwable = null
  ) extends FuturedBodyException(messageWithoutBody, body, ex)
  case class RetrieveBadResponseException(
    override val messageWithoutBody: String,
    override val body: Future[String],
    override val ex: Throwable = null
  ) extends FuturedBodyException(messageWithoutBody, body, ex)
  case class RetrieveTsvBadResponseException(
    override val messageWithoutBody: String,
    override val body: Future[String],
    override val ex: Throwable = null
  ) extends FuturedBodyException(messageWithoutBody, body, ex)

}
