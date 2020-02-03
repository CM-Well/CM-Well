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
package cmwell.util

import java.util.jar._
import java.io.{File, FileInputStream, FileOutputStream, InputStream, OutputStream}
import org.codehaus.plexus.archiver.tar._
import com.typesafe.scalalogging.LazyLogging
import cmwell.util.string._
import cmwell.util.os.Props._
import cmwell.util.exceptions._
import scala.language.implicitConversions

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 7/23/13
  * Time: 10:51 AM
  * To change this template use File | Settings | File Templates.
  */
package object files extends LazyLogging {

  //it's a wrapper for plexus' weird logger...
  //TODO: use the threshold!
  private[this] class FileOpsTmpLogger(threshold: Int, name: String)
      extends org.codehaus.plexus.logging.AbstractLogger(threshold, name)
      with LazyLogging {

    def getChildLogger(arg0: String): org.codehaus.plexus.logging.Logger = { null }

    def fatalError(arg0: String, arg1: Throwable): Unit = logger.error(arg0, arg1)

    def error(arg0: String, arg1: Throwable): Unit = logger.error(arg0, arg1)

    def warn(arg0: String, arg1: Throwable): Unit = logger.warn(arg0, arg1)

    def info(arg0: String, arg1: Throwable): Unit = logger.debug(arg0, arg1)

    def debug(arg0: String, arg1: Throwable): Unit = logger.trace(arg0, arg1)

  }

  def fileFromZipToStream(filename: String, zipfile: String): InputStream = {
    val zip = stringToZipFile(zipfile)
    val ze = {
      val rv = zip.getEntry(filename)
      if (rv == null) {
        val toggleSlash = if (filename.head == '/') filename.drop(1) else s"/$filename"
        zip.getEntry(toggleSlash)
      } else rv
    }
    zip.getInputStream(ze)
  }

  /**
    * @author Israel (copied from clfRunner & modified by Gilad)
    *
    * Copy from InputStream to target file
    */
  implicit def stringToFile(filename: String): File = new java.io.File(filename)
  def isToFile(input: InputStream, to: File) {
    ensureDirHirerchy(to.getAbsolutePath)
    val out = new FileOutputStream(to)
    try {
      isToOs(input, out)
    } finally {
      out.close()
    }
  }

  private[this] def ensureDirHirerchy(path: String) {
    val nixPath = unixify(path)
    new java.io.File(nixPath.dropRight(nixPath.length - nixPath.lastIndexOf("/"))).mkdirs
  }

  def deployDirectoryFromJarToFile(jarPath: String, to: String) {
    logger.debug("deploying " + jarPath + " to " + to)
    val jarfile = new JarFile(jarName)
    logger.debug("jarfile = " + jarfile.getName)
    unzipEntries(jarfile,
                 jarfile.entries,
                 to,
                 e => e.getName.startsWith(jarPath),
                 s => if (s.startsWith(jarPath)) s.drop(jarPath.length) else s)
  }

  /**
    * @author Israel (copied from clfRunner & modified by Gilad)
    *
    * Copy from InputStream to target file through buffer
    */
  def isToOs(input: InputStream, to: OutputStream) {
    val buffer = new Array[Byte](8192)
    def transfer() {
      val read = input.read(buffer)
      if (read >= 0) {
        to.write(buffer, 0, read)
        transfer()
      }
    }
    transfer()
  }

  def isToString(input: InputStream): String = {
    scala.io.Source.fromInputStream(input).mkString
  }

  /**
    * @author Gilad
    *
    * override a file with data from an input stream
    * (delete the file first, even if it's a directory)
    */
  def isToOverrideFile(input: InputStream, to: File) {
    if (to.exists) {
      if (to.isDirectory) {
        cleanDir(to)
      }
      to.delete
    }
    isToFile(input, to)
  }

  /**
    * @author Gilad
    *
    * extract a given zip File into a given destination
    */
  implicit def stringToZipFile(zip: String): java.util.zip.ZipFile = new java.util.zip.ZipFile(zip)
  implicit def fileToZipFile(file: java.io.File): java.util.zip.ZipFile = new java.util.zip.ZipFile(file)
  def unzip(zip: java.util.zip.ZipFile, to: String) {
    unzipEntries(zip, zip.entries, to, e => true, identity)
    zip.close
  }

  private[this] def unzipEntries(zip: java.util.zip.ZipFile,
                                 entries: java.util.Enumeration[_ <: java.util.zip.ZipEntry],
                                 to: String,
                                 filter: java.util.zip.ZipEntry => Boolean,
                                 pathModifier: String => String) {
    while (entries.hasMoreElements) {
      val entry: java.util.zip.ZipEntry = entries.nextElement
      if (filter(entry)) {
        if (entry.isDirectory) {
          val d = new File(to + pathModifier(entry.getName))
          if (!d.exists) { d.mkdirs }
        } else {
          val n = unixify(pathModifier(entry.getName))
          val d = (n.dropRight(n.length - n.lastIndexOf("/")))
          val f = new File(to + d)
          if (!f.exists) { f.mkdirs }
          isToFile(zip.getInputStream(entry), new java.io.File(to + n))
        }
      } else {
        logger.trace("skipping: " + entry.getName)
      }
    }
  }

  /**
    * extract a given tar.gz File into a given destination
    */
  def untgz(srcFile: File, dstDir: File) {
    def validLetters(c: Char): Boolean = {
      c match {
        case '_' => true
        case '-' => true
        case chr => chr.isLetter
      }
    }

    val ua: TarGZipUnArchiver = new TarGZipUnArchiver
    // for some reason, plexus' TarGZipUnArchiver must have org.codehaus.plexus.logging.Logger
    // enabled for it to run properly. (otherwise you'll get a null pointer exception)
    val logger: org.codehaus.plexus.logging.Logger =
      new FileOpsTmpLogger(0, srcFile.getName.toLowerCase.filterNot(validLetters))
    ua.enableLogging(logger)
    ua.setSourceFile(srcFile)
    dstDir.mkdirs
    ua.setDestDirectory(dstDir)
    ua.extract
  }

  def cleanDir(dir: java.io.File) { cleanDir(dir, _ => false) }

  /**
    * filter should return true for files that needs to be kept
    * and not deleted.
    */
  def cleanDir(dir: java.io.File, filter: String => Boolean) {
    //logger.debug("cleanDir: " + dir.getAbsolutePath)

    if (!dir.exists) {
      throw new IllegalFileException("can't clean a dir which does not exist! (" + dir.getAbsolutePath + ")")
    } else if (!dir.isDirectory) {
      throw new IllegalFileException(dir.getAbsolutePath + " is not a directory!")
    }

    for (f <- dir.listFiles) {
      if (!filter(f.getName)) {
        if (f.isDirectory) {
          cleanDir(f, filter)
        }
        f.delete
      }
    }
  }

  /**
    *
    */
  def mergeDirInto(src: String, dst: String) {
    val s = new java.io.File(src)
    if (!s.exists) throw new IllegalArgumentException("source file " + src + " was not found!")
    val d = new java.io.File(dst)
    if (s.isDirectory) {
      if (d.exists && !d.isDirectory) {
        d.delete
      } else if (!d.exists) {
        d.mkdirs
      }
      s.list.foreach(f => {
        mergeDirInto(src + "/" + f, dst + "/" + f);
        logger.trace("merging " + f + " into " + d.getAbsolutePath)
      })
    } else {
      if (d.exists) {
        d.delete
      }
      isToFile(new FileInputStream(s), d)
    }
  }

  def replaceFileContentFromInputStream(contentInputStream: InputStream, fileToOverride: String) {
    val file = new java.io.File(fileToOverride)
    val path =
      new java.io.File(fileToOverride.dropRight(fileToOverride.length - fileToOverride.lastIndexOf(fileSeparator)))

    if (file.exists) {
      file.delete
    } else {
      path.mkdirs
    }

    isToFile(contentInputStream, file)
  }

  def writeStringToFile(s: String, f: File) {
    logger.debug("writing: " + s + " to " + f.getAbsolutePath)

    if (f.exists) {
      logger.warn(f.getName + " already exists!")
      if (!f.delete) {
        logger.error("failed to truncate " + f.getName)
        throw new IllegalFileException("could not delete " + f.getAbsolutePath)
      }
    } else { logger.debug(f.getName + " does not exists.") }

    if (f.createNewFile) {
      logger.debug("creating: " + f.getAbsolutePath)
      val fw = new java.io.FileWriter(f)
      fw.write(s)
      fw.flush
    } else {
      logger.error("failed to create " + f.getName)
      throw new IllegalFileException("could not create " + f.getAbsolutePath)
    }
  }

  def deleteFile(path: String) = java.nio.file.Files.delete(java.nio.file.Paths.get(path))
}
