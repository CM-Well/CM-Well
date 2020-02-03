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
/**
  * Created by matan on 1/8/16.
  */
import java.io.{BufferedInputStream, File}
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import scala.util.control.Breaks._

import javax.xml.bind.DatatypeConverter
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils
object UtilCommands {
  val OSX_NAME = "Mac OS X"

  val linuxSshpass = if (Files.exists(Paths.get("bin/utils/sshpass"))) "bin/utils/sshpass" else "sshpass"
  val osxSshpass = "/usr/local/bin/sshpass"

  val sshpass = if (isOSX) osxSshpass else linuxSshpass

  def isOSX = System.getProperty("os.name") == OSX_NAME

  def verifyComponentConfNotChanged(componentName:String, configFilePath:String, expectedHash:String) = {
    val confContent = UtilCommands.unTarGz("./components", componentName, configFilePath)
    UtilCommands.checksum(componentName, configFilePath, confContent, expectedHash)
  }

  def checksum(componentName:String, configFilePath:String, confContent:Array[Byte], expectedHash:String) = {
    val actualHash = MessageDigest.getInstance("MD5").digest(confContent)
    val actualHashStr = DatatypeConverter.printHexBinary(actualHash)
    if (!expectedHash.equalsIgnoreCase(actualHashStr))
      throw new Exception(s"$componentName configuration file $configFilePath has been changed, please change the template accordingly " +
        s"(the new digest is $actualHashStr)")
  }

  def unTarGz(rootFolder:String, componentName: String, configFilePath:String):Array[Byte] = {
    var tarArchiveInputStream:TarArchiveInputStream = null
    var bufferInputstream:BufferedInputStream = null
    val gzipCompressor:GzipCompressorInputStream = null
    var confContent: Array[Byte] = null
    try {
      val libDir = new File(rootFolder)
      val pathInput = libDir.listFiles().filter(file => file.getName.contains(componentName))
      val path = Paths.get(pathInput(0).getAbsolutePath)
      val bufferInputStream = new BufferedInputStream(Files.newInputStream(path))
      val gzipCompressor = new GzipCompressorInputStream(bufferInputStream)
      tarArchiveInputStream = new TarArchiveInputStream(gzipCompressor)

      var archiveEntry: ArchiveEntry = null
      archiveEntry = tarArchiveInputStream.getNextEntry
      if(archiveEntry.getName == "./")
        archiveEntry = tarArchiveInputStream.getNextEntry

      val extractFolder = archiveEntry.getName.replaceAll("^\\./","").split("/")(0)
      while (archiveEntry != null) {
        breakable {
        if (archiveEntry.getName.replaceAll("^\\./","") == s"$extractFolder/$configFilePath") {
            confContent = IOUtils.toByteArray(tarArchiveInputStream)
            break
          }
        }
        archiveEntry = tarArchiveInputStream.getNextEntry
      }
    }
    finally {
      if(tarArchiveInputStream != null)
        tarArchiveInputStream.close()
      if(bufferInputstream != null)
        bufferInputstream.close()
      if(gzipCompressor != null)
        gzipCompressor.close()
    }
    confContent
  }
}
