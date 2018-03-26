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
/**
  * Created by matan on 1/8/16.
  */
import java.nio.file.{Files, Paths}
object UtilCommands {
  val OSX_NAME = "Mac OS X"

  val linuxSshpass = if (Files.exists(Paths.get("bin/utils/sshpass"))) "bin/utils/sshpass" else "sshpass"
  val osxSshpass = "/usr/local/bin/sshpass"

  val sshpass = if (isOSX) osxSshpass else linuxSshpass

  def isOSX = System.getProperty("os.name") == OSX_NAME
}
