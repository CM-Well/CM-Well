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
import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

object FileHandler {


  def readInputStreamAndFilterMeta(inputStream:InputStream, fileName:String):File = {
    val targetFile = new File(fileName)
    val fw = new FileWriter(targetFile, true)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    reader.lines().filter(line => !line.contains("meta/sys")).iterator.asScala.foreach(line => fw.write(line + "\n"))
    fw.close()
    targetFile
  }


  def deleteTempCmwellFiles(directoryName: String, extension: String): Unit = {
    val directory = new File(directoryName)
    FileUtils.listFiles(directory, new WildcardFileFilter(extension), null).asScala.foreach(_.delete())
  }

}
