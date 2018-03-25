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
import cmwell.dc.LazyLogging
import cmwell.dc.stream.MessagesTypesAndExceptions.FuturedBodyException

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
          case Success(body) => logger.trace(s"${badResponse.getCause.getMessage} body: $body")
          case Failure(e)    => logger.trace(s"${badResponse.getCause.getMessage} body: Couldn't get it. Exception: ", e)
        }
      case _ =>
    }
  }

  def infoPrintFuturedBodyException(ex: Throwable): Unit = {
    ex match {
      case badResponse: FuturedBodyException =>
        badResponse.body.onComplete {
          case Success(body) => logger.info(s"${badResponse.getCause.getMessage} body: $body")
          case Failure(e)    => logger.info(s"${badResponse.getCause.getMessage} body: Couldn't get it. Exception: ", e)
        }
      case _ =>
    }
  }

  def errorPrintFuturedBodyException(ex: Throwable): Unit = {
    ex match {
      case badResponse: FuturedBodyException =>
        badResponse.body.onComplete {
          case Success(body) => logger.error(s"${badResponse.getCause.getMessage} body: $body")
          case Failure(e)    => logger.error(s"${badResponse.getCause.getMessage} body: Couldn't get it. Exception: ", e)
        }
      case _ =>
    }
  }

  def warnPrintFuturedBodyException(ex: Throwable): Unit = {
    ex match {
      case badResponse: FuturedBodyException =>
        badResponse.body.onComplete {
          case Success(body) => logger.warn(s"${badResponse.getCause.getMessage} body: $body")
          case Failure(e)    => logger.warn(s"${badResponse.getCause.getMessage} body: Couldn't get it. Exception: ", e)
        }
      case _ =>
    }
  }

  def headerString(header: HttpHeader): String = header.name + ":" + header.value

  def headersString(headers: Seq[HttpHeader]): String = headers.map(headerString).mkString("[", ",", "]")

  def decodeResponse(response: HttpResponse): HttpResponse = {
    val decoder = response.encoding match {
      case HttpEncodings.gzip ⇒
        Gzip
      case HttpEncodings.deflate ⇒
        Deflate
      case HttpEncodings.identity ⇒
        NoCoding
    }
    decoder.decodeMessage(response)
  }
}
