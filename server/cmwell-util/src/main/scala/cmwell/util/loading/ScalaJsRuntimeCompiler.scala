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
package cmwell.util.loading

import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process._

/**
  * Created by yaakov on 9/5/16.
  */
object ScalaJsRuntimeCompiler {
  private val workingDir = System.getProperty("java.io.tmpdir") + "/sjsbuild"
  private val baseFileContent = Seq(
    "build.sbt" -> """ enablePlugins(ScalaJSPlugin); scalaVersion := "2.11.8"  """,
    "project/plugins.sbt" -> """ addSbtPlugin("org.scala-js" % "sbt-scalajs" % "0.6.12") """,
    "project/build.properties" -> """ sbt.version=0.13.9 """
  )

  // TODO *MUST* ADD A CACHE LAYER!!! PREFERABLE IN CAS

  /**
    *
    * @param scalaContent ScalaJS code as String
    * @param ec implicit executioncontext
    * @return String of JavaScript code (compiled ScalaJS)
    *
    * `package` is not supported! Do not place your ScalaJS in packages.
    *
    */
  def compile(scalaContent: String)(implicit ec: ExecutionContext): Future[String] = {
    val filesContent = baseFileContent :+ "src/main/scala/sourceFile.scala" -> scalaContent
    val subfolders =
      filesContent.filter(_._1.contains('/')).map { case (path, _) => path.substring(0, path.lastIndexOf('/')) }

    Future {
      subfolders.foreach(subfolder => Files.createDirectories(Paths.get(s"$workingDir/$subfolder")))
      filesContent.foreach {
        case (filename, contents) =>
          Files.write(Paths.get(s"$workingDir/$filename"), contents.getBytes("UTF-8"))
      }
      Process(Seq("sbt", "fastOptJS"), new java.io.File(workingDir)).!
      scala.io.Source.fromFile(s"$workingDir/target/scala-2.11/sjsbuild-fastopt.js").mkString("")
    }
  }
}
