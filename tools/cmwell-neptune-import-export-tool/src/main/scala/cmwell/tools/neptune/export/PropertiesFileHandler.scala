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

import java.io._
import java.nio.file.{Files, Paths}
import java.util.Properties
import net.liftweb.json.DefaultFormats

object PropertiesFileHandler{
  implicit val formats = DefaultFormats
  val directory = "/tmp/cm-well/"
  val fileName = "config.properties"
  val AUTOMATIC_UPDATE_MODE = "automaticUpdateMode"
  val POSITION  = "position"
  val START_TIME = "start_time"

  def persistPosition(position: String):Unit = {
    var prop: Properties = new Properties()
    var output: OutputStream = null
    try {
      prop.setProperty(PropertiesFileHandler.POSITION, position)
      readKey(START_TIME).foreach(x => prop.setProperty(START_TIME, x))
      readKey(PropertiesFileHandler.AUTOMATIC_UPDATE_MODE).foreach(x => prop.setProperty(PropertiesFileHandler.AUTOMATIC_UPDATE_MODE, x))
      output = new FileOutputStream(fileName)
      prop.store(output, null)
    } catch {
      case io: IOException =>
        io.printStackTrace();
    } finally {
      if (output != null) {
        try {
          output.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }

    }
  }

  def persistStartTime(startTime: String):Unit = {
    var prop: Properties = new Properties()
    var output: OutputStream = null
    try {
      prop.setProperty(START_TIME, startTime)
      readKey(PropertiesFileHandler.POSITION).foreach(x => prop.setProperty(PropertiesFileHandler.POSITION, x))
      readKey(PropertiesFileHandler.AUTOMATIC_UPDATE_MODE).foreach(x => prop.setProperty(PropertiesFileHandler.AUTOMATIC_UPDATE_MODE, x))
      output = new FileOutputStream(fileName)
      prop.store(output, null);

    } catch {
      case io: IOException => io.printStackTrace();
    } finally {
      if (output != null) {
        try {
          output.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
  }

  def persistAutomaticUpdateMode(automaticUpdateMode: Boolean): Unit = {
    var prop: Properties = new Properties()
    var output: OutputStream = null

    try {
      prop.setProperty(PropertiesFileHandler.AUTOMATIC_UPDATE_MODE, automaticUpdateMode.toString)
      readKey(PropertiesFileHandler.POSITION).foreach(x => prop.setProperty(PropertiesFileHandler.POSITION, x))
      readKey(START_TIME).foreach(x => prop.setProperty(START_TIME, x))
      output = new FileOutputStream(fileName)
      prop.store(output, null);

    } catch {
      case io: IOException =>
        io.printStackTrace();
    } finally {
      if (output != null) {
        try {
          output.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
  }

  def readKey(key:String):Option[String] = {
    if(!isPropertyFileExists) return None
    var prop = new Properties()
    var input:InputStream = null
    input = new FileInputStream(fileName)
    prop.load(input)
    Option(prop.getProperty(key))
  }

  def isPropertyPersist(property:String):Boolean ={
      if(isPropertyFileExists && readKey(property).isDefined) true else false
  }

  def isPropertyFileExists:Boolean ={
    Files.exists(Paths.get(fileName))
  }


  def createDirectoryIfNotExists() = {
    val cmWellDirectory = new File(directory)
    if (!cmWellDirectory.exists)
        cmWellDirectory.mkdirs()
  }
}