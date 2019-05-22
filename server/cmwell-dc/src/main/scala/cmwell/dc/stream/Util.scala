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

import akka.http.scaladsl.model.{HttpHeader, HttpResponse}
import akka.http.scaladsl.coding.{Deflate, Gzip, NoCoding}
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.util.ByteString
import cmwell.dc.LazyLogging
import cmwell.dc.stream.MessagesTypesAndExceptions.{DcInfo, FuturedBodyException, InfotonData}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * Created by eli on 17/07/16.
  */
object Util extends LazyLogging {
  def tracePrintFuturedBodyException(ex: Throwable): Unit = {
    ex match {
      case badResponse: FuturedBodyException =>
        badResponse.body.onComplete {
          case Success(body) =>
            logger.trace(s"${badResponse.getCause.getMessage} body: $body")
          case Failure(e) =>
            logger.trace(
              s"${badResponse.getCause.getMessage} body: Couldn't get it. Exception: ",
              e
            )
        }
      case _ =>
    }
  }

  def infoPrintFuturedBodyException(ex: Throwable): Unit = {
    ex match {
      case badResponse: FuturedBodyException =>
        badResponse.body.onComplete {
          case Success(body) =>
            logger.info(s"${badResponse.getCause.getMessage} body: $body")
          case Failure(e) =>
            logger.info(
              s"${badResponse.getCause.getMessage} body: Couldn't get it. Exception: ",
              e
            )
        }
      case _ =>
    }
  }

  def errorPrintFuturedBodyException(ex: Throwable): Unit = {
    ex match {
      case badResponse: FuturedBodyException =>
        badResponse.body.onComplete {
          case Success(body) =>
            logger.error(s"${badResponse.getCause.getMessage} body: $body")
          case Failure(e) =>
            logger.error(
              s"${badResponse.getCause.getMessage} body: Couldn't get it. Exception: ",
              e
            )
        }
      case _ =>
    }
  }

  def warnPrintFuturedBodyException(ex: Throwable): Unit = {
    ex match {
      case badResponse: FuturedBodyException =>
        badResponse.body.onComplete {
          case Success(body) =>
            logger.warn(s"${badResponse.getCause.getMessage} body: $body")
          case Failure(e) =>
            logger.warn(
              s"${badResponse.getCause.getMessage} body: Couldn't get it. Exception: ",
              e
            )
        }
      case _ =>
    }
  }

  def headerString(header: HttpHeader): String =
    header.name + ":" + header.value

  def headersString(headers: Seq[HttpHeader]): String =
    headers.map(headerString).mkString("[", ",", "]")

  def createInfotonDataTransformer(dcInfo: DcInfo): InfotonData => InfotonData = {
    if (dcInfo.transformations.isEmpty) identity
    else {
      val transformations = dcInfo.transformations.toList
      infotonData => {
        val oldMeta = infotonData.meta
        val newMeta = oldMeta.copy(path = transform(transformations, oldMeta.path))
        val infotonQuads = infotonData.data.utf8String.split('\n')
        val newData = infotonQuads.foldLeft(StringBuilder.newBuilder) { (total, line) =>
          val subjectEndPos = line.indexOf(' ')
          val predicateEndPos = line.indexOf(' ', subjectEndPos + 1)
          val isValueReference = line.charAt(predicateEndPos + 1) == '<'
          val lastSpaceBeforeLastPart = line.lastIndexOf(' ', line.length - 3)
          val isLastReference = line.charAt(line.length - 3) == '>' && !line.substring(lastSpaceBeforeLastPart).contains("^^")
          val isQuad = isLastReference && lastSpaceBeforeLastPart != predicateEndPos
          val valueEndPos = if (isQuad) lastSpaceBeforeLastPart else line.length - 2
          val newSubject = transform(transformations, line.substring(0, subjectEndPos + 1))
          val predicate = line.substring(subjectEndPos + 1, predicateEndPos + 1)
          val oldValue = line.substring(predicateEndPos + 1, valueEndPos + 1)
          val newValue = if (isValueReference) transform(transformations, oldValue) else oldValue
          val newQuad = if (isQuad) transform(transformations, line.substring(valueEndPos + 1)) else line.substring(valueEndPos + 1)
          total ++= newSubject ++= predicate ++= newValue ++= newQuad += '\n'
        }
        InfotonData(newMeta, ByteString(newData.result()))
      }
    }
  }

  def transform(transformations: List[(String, String)], str: String): String = {
    transformations.foldLeft(str)((result, kv) => result.replace(kv._1, kv._2))
  }

}
