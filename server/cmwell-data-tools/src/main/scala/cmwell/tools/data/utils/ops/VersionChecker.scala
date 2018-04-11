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
package cmwell.tools.data.utils.ops

import cmwell.tools.data.utils.ops
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.{Sink, Source}
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.logging.DataToolsLogging
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

object VersionChecker extends DataToolsLogging with DataToolsConfig {
  val remoteVersionHost = config.getString("cmwell.remote-version-host")
  val remoteVersionPath = config.getString("cmwell.remote-version-path")
  var wasChecked = false

//  printIfNeedUpdate()

  private def getVersionFromRemote() = {
    import cmwell.tools.data.utils.akka.Implicits._
    import scala.concurrent.ExecutionContext.Implicits.global

    logger.info("checking if new cmwell-data-tools version is available")

    val conn = Http().newHostConnectionPool[Option[_]](host = remoteVersionHost)

    Source
      .single(HttpRequest(uri = s"http://$remoteVersionHost$remoteVersionPath?format=ntriples") -> None)
      .via(conn)
      .mapAsync(1) {
        case (Success(HttpResponse(status, _, entity, _)), _) if status.isSuccess() =>
          entity.dataBytes.runFold(blank)(_ ++ _).map(Option.apply)
        case (Success(HttpResponse(status, _, entity, _)), _) =>
          entity.discardBytes()
          Future.successful(None)
        case _ =>
          logger.error("cannot get remote version")
          Future.successful(None)
      }
      .runWith(Sink.head)
  }

  private def printIfNeedUpdate() = if (!wasChecked) {
    wasChecked = true
    val currentVersion = System.getProperty("prog.version", ops.getVersionFromManifest()) match {
      case null                                                  => None
      case version if version.toLowerCase().contains("snapshot") => None
      case version                                               => Some(version)
    }

    val remoteData = Try { Await.result(getVersionFromRemote(), 10.seconds) }

    val remoteVersion = remoteData match {
      case Success(Some(data)) =>
        Some(
          data.utf8String.lines
            .filter(_.contains("#version>"))
            .map(line => line.substring(line.lastIndexOf('>') + 3, line.size - 3))
            .mkString
        )
      case Success(None) =>
        None
      case Failure(err) =>
        None
      case _ =>
        None
    }

    for {
      current <- currentVersion
      remote <- remoteVersion
      if remote.nonEmpty
      if current != remote
    } {
      System.err.println(
        """
          |.__   __.  ___________    __    ____    ____    ____  _______ .______          _______. __    ______   .__   __.
          ||  \ |  | |   ____\   \  /  \  /   /    \   \  /   / |   ____||   _  \        /       ||  |  /  __  \  |  \ |  |
          ||   \|  | |  |__   \   \/    \/   /      \   \/   /  |  |__   |  |_)  |      |   (----`|  | |  |  |  | |   \|  |
          ||  . `  | |   __|   \            /        \      /   |   __|  |      /        \   \    |  | |  |  |  | |  . `  |
          ||  |\   | |  |____   \    /\    /          \    /    |  |____ |  |\  \----.----)   |   |  | |  `--'  | |  |\   |
          ||__| \__| |_______|   \__/  \__/            \__/     |_______|| _| `._____|_______/    |__|  \______/  |__| \__|
          |
          |              ___   ____    ____  ___       __   __          ___      .______    __       _______
          |             /   \  \   \  /   / /   \     |  | |  |        /   \     |   _  \  |  |     |   ____|
          |            /  ^  \  \   \/   / /  ^  \    |  | |  |       /  ^  \    |  |_)  | |  |     |  |__
          |           /  /_\  \  \      / /  /_\  \   |  | |  |      /  /_\  \   |   _  <  |  |     |   __|
          |          /  _____  \  \    / /  _____  \  |  | |  `----./  _____  \  |  |_)  | |  `----.|  |____
          |         /__/     \__\  \__/ /__/     \__\ |__| |_______/__/     \__\ |______/  |_______||_______|  """.stripMargin + remote
      )
    }
  }
}
