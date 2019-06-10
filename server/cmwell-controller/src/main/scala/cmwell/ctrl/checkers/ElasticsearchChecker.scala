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
package cmwell.ctrl.checkers

import cmwell.ctrl.config.Config
import cmwell.ctrl.utils.ProcUtil
import cmwell.util.http.{SimpleResponse, SimpleHttpClient => Http}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Future
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by michael on 12/3/14.
  */
object ElasticsearchChecker extends Checker with LazyLogging {
  override val storedStates: Int = 10

  def getHealth (port: Int):Future[SimpleResponse[Array[Byte]]] = {

    val url = s"http://${Config.pingIp}:$port/_cluster/health"
    val esRes = Http.get(url)

    esRes.flatMap{res => res match {
      case r if r.status == 200 => esRes
      case _ if port > 9203 => {logger.error(s"Status was ${res.status} and not 200 for port: $port. Stop retrying."); esRes}
      case _ => logger.warn(s"Status was ${res.status} and not 200 for port: $port. Increasing port by 1."); getHealth(port + 1)
    }}.recoverWith{
      case exception : Exception => {
        logger.error(s"Getting ES health failed with exception.", exception)
        if(port > 9203) {
          logger.error(s"Reached port $port Stop retrying.")
          Future.failed(exception)
        }
        else
          getHealth(port + 1)
      }
    }

  }

  override def check: Future[ComponentState] = {
    val res = getHealth(9201)
    val hasMaster = ProcUtil.checkIfProcessRun("es-master") > 0
    val isCoorUp = ProcUtil.checkIfProcessRun("es-coordinator") > 0
    res.map { r =>
        if (r.status == 200) {
          val json: JsValue = Json.parse(r.payload)
          val status = json.\("status").as[String]
          val n = (json \ "number_of_nodes").as[Int]
          val d = (json \ "number_of_data_nodes").as[Int]
          val p = (json \ "active_primary_shards").as[Int]
          val s = (json \ "active_shards").as[Int](implicitly)
          status match {
            case "green"  => ElasticsearchGreen(n, d, p, s, hasMaster, isCoorUp)
            case "yellow" => ElasticsearchYellow(n, d, p, s, hasMaster, isCoorUp)
            case "red"    => ElasticsearchRed(n, d, p, s, hasMaster, isCoorUp)
            case _        => throw new Exception("Bad status")
          }
        } else
          ElasticsearchBadCode(r.status, hasMaster)
      }
      .recover {
        case e: Throwable => {
          logger.error("ElasticsearchChecker check failed with an exception: ", e)
          ElasticsearchDown(hasMaster)
        }
      }
  }
}
