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
package cmwell.ctrl.utils

/**
  * Created by michael on 6/10/15.
  */
import java.io.File
import java.nio.file._
import java.nio.file.StandardWatchEventKinds._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

case class DirWatcher(dirPath: String, events: Seq[java.nio.file.WatchEvent.Kind[_]])(
  callBack: (String, java.nio.file.WatchEvent.Kind[_]) => Unit
) {
  private var key: WatchKey = _
  private var isRunning = true

  def watch {
    val file = Paths.get(dirPath)
    val watcher = FileSystems.getDefault.newWatchService

    // check if the directory exists. If not create one.
    if (!Files.exists(Paths.get(dirPath))) {
      val dir = new File(dirPath)
      dir.mkdir()
    }

    file.register(
      watcher,
      events: _*
    )

    def doWatch: Unit = {
      blocking {
        Future {
          while (isRunning) {
            key = watcher.take

            val events = key.pollEvents
            events.asScala.foreach { event =>
              val path = event.context().asInstanceOf[Path]
              callBack(path.toString, event.kind())
            }
          }
        }
      }
    }

    doWatch
  }

  def stop {
    isRunning = false
    key.cancel()
  }
}
