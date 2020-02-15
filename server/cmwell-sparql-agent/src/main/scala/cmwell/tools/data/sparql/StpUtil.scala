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
package cmwell.tools.data.sparql

import cmwell.tools.data.utils.akka.stats.DownloaderStats.DownloadStats
import cmwell.tools.data.utils.akka.stats.IngesterStats.IngestStats
import cmwell.tools.data.utils.logging.DataToolsLogging
import cmwell.zstore.ZStore
import io.circe._
import io.circe.parser._
import cmwell.util.concurrent.{DoNotRetry, RetryParams, RetryWith, ShouldRetry, retryUntil}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object StpUtil extends DataToolsLogging {
  def headerString(header: (String, String)): String = header._1 + ":" + header._2

  def headersString(headers: Seq[(String, String)]): String = headers.map(headerString).mkString("[", ",", "]")

  def extractLastPart(path: String) = {
    val p =
      if (path.endsWith("/")) path.init
      else path
    val (_, name) = p.splitAt(p.lastIndexOf("/"))
    name.tail
  }

  def readPreviousTokensWithRetry(baseUrl: String, path: String, format: String, zStore: ZStore)
                                 (implicit context: ExecutionContext) = {

    def shouldRetry(action: String): (Try[AgentTokensAndStatistics], RetryParams) => ShouldRetry[RetryParams] = {
      import scala.language.implicitConversions
      implicit def asFiniteDuration(d: Duration) = scala.concurrent.duration.Duration.fromNanos(d.toNanos);
      {
        case (Success(_),_) =>
          logger.debug(s"Successfully read token and statistics state from zStore for agent ${extractLastPart(path)}")
          DoNotRetry
        case (Failure(ex), state) if state.retriesLeft == 0 => {
          logger.error(ex.getMessage)
          logger.error(s"Failed to read token and statistics state from zStore. 0 retries left. Will not re-attempt for agent ${extractLastPart(path)}")
          DoNotRetry
        }
        case (Failure(ex), state @ RetryParams(retriesLeft, delay, delayFactor)) => {
          logger.error(ex.getMessage)
          logger.warn(s"Failed to read token and statistics state from zStore. " +
            s"$retriesLeft retries left for agent ${extractLastPart(path)}")
          val newDelay = delay * delayFactor
          RetryWith(state.copy(delay = newDelay, retriesLeft = retriesLeft - 1))
        }

      }
    }

    retryUntil(RetryParams(3, 5.seconds, 1))(shouldRetry(s"Getting token and statistics state from zStore for agent ${extractLastPart(path)}")) {
      readPreviousTokens(baseUrl,path,zStore)
    }

  }

  def readPreviousTokens(baseUrl: String, path: String, zStore: ZStore)
                        (implicit context: ExecutionContext) : Future[AgentTokensAndStatistics]  = {

    zStore.getStringOpt(s"stp-agent-${extractLastPart(path)}").map {
      case None => {
        // No such key - start STP from scratch
        AgentTokensAndStatistics(Map.newBuilder[String, TokenAndStatistics].result())
      }
      case Some(tokenPayload) => {
        // Key exists and has returned
        val allJson = tokenPayload.lines.map{
          row =>
            parse(row) match {
              case Left(parseFailure@ParsingFailure(_, _)) => throw parseFailure
              case Right(json) => json
            }
        }.toList

        val ingestStats = allJson.find{  _.hcursor.downField("ingestedInfotons").succeeded }.map { json =>
          val ingestedInfotons = json.hcursor.downField("ingestedInfotons").as[Long].toOption
          val failedInfotons = json.hcursor.downField("failedInfotons").as[Long].toOption
          IngestStats(None,0,ingestedInfotons.get,failedInfotons.get)
        }

        val materializedStats = allJson.find{ _.hcursor.downField("materializedInfotons").succeeded }.flatMap { json=>
          json.hcursor.downField("materializedInfotons").as[Long].toOption
        }.getOrElse(0L)

        val totalRunningMillis = allJson.find{ _.hcursor.downField("totalRunningMillis").succeeded }.flatMap { json =>
          json.hcursor.downField("totalRunningMillis").as[Long].toOption
        }.getOrElse(0L)

        val agentStats = Option(DownloadStats(totalRunningTime = totalRunningMillis,receivedInfotons = materializedStats))

        val sensors = allJson.map { json=>
          val token = json.hcursor.downField("token").as[String].getOrElse("")
          val sensor = json.hcursor.downField("sensor").as[String].getOrElse("")
          val receivedInfotons = json.hcursor.downField("receivedInfotons").as[Long].toOption.map {
            value => DownloadStats(receivedInfotons = value)
          }
          sensor -> (token, receivedInfotons)
        }.foldLeft(Map.newBuilder[String, TokenAndStatistics])(_.+=(_))
        .result

        AgentTokensAndStatistics(sensors, ingestStats, agentStats)

      }
    }
  }
}
