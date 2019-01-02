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
package cmwell.tools.neptune.export

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import scala.io.Source


object PositionFileHandler{
  val directory = "/tmp/cm-well"
  val fileName = "position"
  val filePath = directory + "/" + fileName

  def persistPosition(lastPosition:String) = {
    createDirectoryIfNotExists()
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(lastPosition.toString)
    bw.close()

  }


  def readPosition = {
    val source = Source.fromFile(filePath)
    try { source.getLines().toList.head }
    finally { source.close() }
  }

  def isPositionPersist():Boolean ={
    Files.exists(Paths.get(filePath))
  }

  def createDirectoryIfNotExists() = {
    val cmWellDirectory = new File(directory)
    if (!cmWellDirectory.exists)
        cmWellDirectory.mkdirs()
  }

}