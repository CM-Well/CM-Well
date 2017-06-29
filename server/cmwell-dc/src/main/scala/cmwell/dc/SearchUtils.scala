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


package cmwell.dc

import akka.actor.Actor
import akka.actor.Status.Failure
import akka.io.IO
import spray.can.Http
import spray.client.pipelining._
import spray.http._

import spray.json._

import scala.util.Try

/*
 * Created by markz on 6/22/15.
 */

sealed abstract class Type

object Remote extends Type
object Local extends Type

case class DcInfo(dataCenterId : String , t : Type ,location : String )

case object Retrieve
case class DcData(d : Seq[DcInfo])

// when empty no data was found
case class ScanRes(dcData : DcData)

class RetrieveDcList(hostname : String ) extends Actor with LazyLogging {
  val io = IO(Http)(context.system)

  def uri = s"http://${hostname}/meta/sys/dc?op=search&with-data&format=json&pretty"

  def extractDcInfo(buffer : String ) : Seq[DcInfo] = {
    try {
      val js = buffer.parseJson
      val arr = js.asJsObject.fields("results").asJsObject.fields("infotons")
      val a  : Seq[DcInfo] = arr match {
        case JsArray(data) => data.map {
          d => val f = d.asJsObject.fields("fields")
            val location = f.asJsObject.fields("location") match {
              case JsArray (Vector(JsString (loc))) =>
                loc
            }
            val dataCenterId = f.asJsObject.fields("id") match {
              case JsArray (Vector(JsString (id))) =>
                id
            }
            val t  = f.asJsObject.fields("type") match {
              case JsArray (Vector(JsString (tt))) =>
                tt match {
                  case "local" => Local
                  case "remote" => Remote
                }
            }
            DcInfo(dataCenterId, t , location)
        }
        case _ => Seq.empty
      }
      a
    } catch {
      case e: Exception =>
        Seq.empty
    }
  }

  def receive: Receive = {
    case Retrieve =>
      logger.info(s"connecting to uri ${uri}")
      val rq = HttpRequest(HttpMethods.GET, uri = uri )
      sendTo(io).withResponsesReceivedBy(self)(rq)
    case HttpResponse(s,e,h,p) =>
      logger.debug(s"HttpResponse(${s},${e},${h},${p})")
      if (s.isSuccess) {
        val a = extractDcInfo(e.asString)
        logger.info(s"OK ${context.parent}.")
        context.parent ! DcData(a)
      } else {
        logger.error(s"Error ${context.parent} ${s}.")
        // when there is error send empty
        context.parent ! DcData(Seq.empty[DcInfo])
      }
    case Failure(e) =>
      logger.error(s"1. Failure(${e}})")
      context.parent ! DcData(Seq.empty[DcInfo])
    case e =>
      logger.error(s"error ${e}")
      context.parent ! DcData(Seq.empty[DcInfo])
  }
}

trait LocalStatus
case class LocalStatusForDC(dataCenterId: String, lastIndexTime: Long) extends LocalStatus
case class LocalErrorStatusForDC(dataCenterId: String, error: String) extends LocalStatus

class LastIndexTimeRetriver(hostname : String , dataCenterId : String ) extends Actor with LazyLogging {

  val io = IO(Http)(context.system)

  def uri = s"http://${hostname}/proc/dc/${dataCenterId}?format=json"

  def receive: Receive = {
    case Retrieve =>
      logger.info(s"connecting to uri ${uri}")
      val rq = HttpRequest(HttpMethods.GET, uri = uri )
      sendTo(io).withResponsesReceivedBy(self)(rq)
    case HttpResponse(s,e,h,p) =>
      logger.info(s"HttpResponse(${s},${e},${h},${p})")
      if ( s == StatusCodes.OK ) {
        val a = extractDcInfo(e.asString)
        logger.info(s"OK ${context.parent}.")
        context.parent ! a
      } else {
        logger.error(s"Error ${context.parent} ${s}.")
        // when there is error send empty
        context.parent ! LocalErrorStatusForDC(dataCenterId, s"got status code: ${s.toString}")
      }
    case Failure(e) =>
      logger.error(s"1. Failure(${e}})")
      context.parent ! LocalErrorStatusForDC(dataCenterId, s"got exception: ${e.getMessage}")
    case e =>
      logger.error(s"error ${e}")
      context.parent ! LocalErrorStatusForDC(dataCenterId, s"got unrecognized message: $e")
  }

  def extractDcInfo(jString: String): LocalStatus = Try{
    val jObj = jString.parseJson.asJsObject.fields("fields").asJsObject
    val dc = (jObj.fields("dc"): @unchecked) match {
      case JsArray(seq) => seq.headOption.collect{case s:JsString => s.value}.get
    }
    val it = (jObj.fields("lastIdxT"): @unchecked) match {
      case JsArray(seq) => seq.headOption.collect{case n:JsNumber => n.value.longValue}.get
    }
    LocalStatusForDC(dc, it)
  }.recover{
    case t: Throwable => LocalErrorStatusForDC("",t.getMessage)
  }.get
}


