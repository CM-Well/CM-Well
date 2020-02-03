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
package cmwell.ctrl.controllers

import cmwell.ctrl.config.Config
import cmwell.ctrl.utils.ProcUtil
import com.typesafe.scalalogging.LazyLogging
import k.grid.Grid

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Try, Success}

/**
  * Created by michael on 2/16/15.
  */
object CassandraController
    extends ComponentController(s"${Config.cmwellHome}/app/cas/cur", "/log/cas[0-9]*", Set("cas", "ccl"))
    with LazyLogging {

  def checkAndRemove: Unit = {
    logger.info("Removing Cassandra down nodes")
    val downNodesTry = {
      val t = blocking {
        val c = Config.cmwellHome
        ProcUtil.executeCommand(
          s"""JAVA_HOME=$c/app/java/bin $c/app/cas/cur/bin/nodetool status 2> /dev/null | grep DN | awk '{print $$2 " " $$7}'"""
        )
      }

        t.flatMap { s =>
          val lb = List.newBuilder[(String, String)]
          s.trim.split("\n").foldLeft(Try(lb)) {
            case (tb, dn) => tb.flatMap { b =>
              Try {
                val dnsplt = dn.split(" ")
                b += ((dnsplt(0), dnsplt(1)))
              }
            }
          }.map(_.result())


          //          a.toList.map { dn =>
          //            try {
          //              val dnsplt = dn.split(" ")
          //              dnsplt(0) -> dnsplt(1)
          //            } catch {
          //              case t: Throwable =>
          //                logger.error("Couldn't get down nodes.", t)
          //                return
          //            }
          //
          //          }
        }
    }

    downNodesTry match {
      case Failure(e) => logger.error("Couldn't get down nodes.", e)
      case Success(downNodes) => try {
        if (downNodes.nonEmpty) {
          downNodes.foreach { dn =>
            logger.info(s"Removing cassandra node: $dn")
            Future {
              blocking {
                ProcUtil.executeCommand(
                  s"JAVA_HOME=${Config.cmwellHome}/app/java/bin ${Config.cmwellHome}/app/cas/cur/bin/nodetool removenode ${dn._2} 2> /dev/null"
                )
              }
            }
          }
          Grid.system.scheduler.scheduleOnce(10.seconds) {
            checkAndRemove
          }
        }
      } catch {
        case t: Throwable =>
          logger.error("Couldn't remove down nodes.", t)
          Grid.system.scheduler.scheduleOnce(10.seconds) {
            checkAndRemove
          }
      }
    }
  }

  def removeCassandraDownNodes {
    Future(checkAndRemove)
  }

}
