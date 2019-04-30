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
import java.io.{BufferedInputStream, File, IOException}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

import javax.xml.bind.DatatypeConverter
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.FileUtils
object UtilCommands {
  val OSX_NAME = "Mac OS X"

  val linuxSshpass = if (Files.exists(Paths.get("bin/utils/sshpass"))) "bin/utils/sshpass" else "sshpass"
  val osxSshpass = "/usr/local/bin/sshpass"

  val sshpass = if (isOSX) osxSshpass else linuxSshpass

  def isOSX = System.getProperty("os.name") == OSX_NAME

  def verifyComponentConfNotChanged(componentName:String, configFilePath:String, expectedHash:String) = {
    val extractedFolder = UtilCommands.unTarGz(componentName, Paths.get("./components"))
    UtilCommands.checksum(s"$extractedFolder/$configFilePath", expectedHash)
    FileUtils.deleteDirectory(new File(extractedFolder))
  }

  def checksum(componentPath:String, expectedHash:String) = {
    val actualConf = Files.readAllBytes(Paths.get(componentPath))
    val actualHash = MessageDigest.getInstance("MD5").digest(actualConf)
    val actualHashStr =  DatatypeConverter.printHexBinary(actualHash)
    if (!expectedHash.equalsIgnoreCase(actualHashStr))
      throw new Exception(s"yaml configuration $componentPath has been changed, please change template accordingly")
  }
  @throws[IOException]
  def unTarGz(componentPath: String, pathOutput: Path): String = {
    val libDir = new File("./components")
    val pathInput = libDir.listFiles().filter(file => file.getName.contains(componentPath))
    val path = Paths.get(pathInput(0).getAbsolutePath)
    val tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(Files.newInputStream(path))))
    var archiveEntry:ArchiveEntry = null
    archiveEntry = tarArchiveInputStream.getNextEntry
    val extractFolder = s"./components/${archiveEntry.getName.split("/")(0)}"
    Files.createDirectory(Paths.get(extractFolder))
    while (archiveEntry != null) {
      val pathEntryOutput = pathOutput.resolve(archiveEntry.getName)
      if(archiveEntry.isDirectory) {
        if(!Files.exists( pathEntryOutput)) {
          Files.createDirectory(pathEntryOutput)
        }
      }else
        Files.copy(tarArchiveInputStream, pathEntryOutput, REPLACE_EXISTING)
      archiveEntry = tarArchiveInputStream.getNextEntry
    }
    tarArchiveInputStream.close()
    extractFolder
  }
}
