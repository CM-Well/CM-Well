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
/*
package cmwell.util

// scalastyle:off
import sun.tools.jps.Arguments
import sun.jvmstat.monitor._
// scalastyle:on
import org.apache.commons.io.IOUtils

import java.io.{InputStream, OutputStream, PrintStream}
import scala.sys.process._
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.sys.process.{ProcessBuilder, ProcessLogger}
import com.typesafe.scalalogging.LazyLogging

import cmwell.util.os.Props._
import cmwell.util.concurrent._
import cmwell.util.exceptions._
import scala.language.postfixOps

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 7/23/13
  * Time: 9:51 AM
  * To change this template use File | Settings | File Templates.
  */
package object process extends LazyLogging {

  def exec(args: Seq[String], environmentVariables: Seq[(String, String)] = Seq(), home: Option[String] = None) = {
    import scala.collection.JavaConverters._

    val cmd: java.util.List[String] = args.asJava
    val pb = new java.lang.ProcessBuilder(cmd)

    home match {
      case Some(dir) => pb.directory(new java.io.File(dir))
      case _         => /* DO NOTHING */
    }

    val env = pb.environment
    environmentVariables.foreach {
      case (variableName, variableValue) => env.put(variableName, variableValue)
    }

    val p = pb.start
    p.getOutputStream.close
    p.getInputStream.close
    p.getErrorStream.close
  }

  def pipe(src: InputStream, dest: OutputStream) {
    @tailrec
    def transfer(s: InputStream, d: OutputStream, i: Int) {
      if (i != -1) {
        d.write(i)
        d.flush()
        transfer(s, d, s.read)
      }
    }

    startOnThread {
      transfer(src, dest, src.read)
    }
  }

  def drainStreamUntil(inputStream: java.io.InputStream, stopString: String) = {

    stopString match {
      case null => {
        while (inputStream.read != -1) {}
      }
      case _ => {
        val sb: StringBuilder = new StringBuilder
        while (sb.toString != stopString) {
          val ch = inputStream.read.toChar
          if (ch == '\n' || ch == '\r') { sb.clear } else if (sb.length == stopString.length) {
            val str = sb.toString.tail
            sb.clear
            sb.appendAll(str)
          }
          sb.append(ch)
        }
      }
    }
  }

  def injectToStream(outputStream: java.io.OutputStream, injectionString: String) = {
    val writer = new java.io.BufferedWriter(new java.io.OutputStreamWriter(outputStream))
    writer.write(injectionString)
    writer.flush()
  }

  private def mapContainsSuffixOf(map: Map[String, String], text: String): Boolean = {
    map.keySet.exists { k =>
      k.length <= text.length && k == text.drop(text.length - k.length)
    }
  }

  private def getKeyThatIsSuffixOf(map: Map[String, String], text: String): String = {
    map.keySet.filter { k =>
      k.length <= text.length && k == text.drop(text.length - k.length)
    }.toList match {
      case Nil =>
        throw new NoneOfTheKeysMatchThisTextSuffixException("text is: " + text + "\n\tkeys are: " + map.keySet.toString)
      case noneEmptyList => noneEmptyList(0)
    }
  }

  /**
    * NOT TERMINATIMG! use on a spawned thread with care!
    * also, replacementMap keys should NOT contain '\r' characters!!!
    * also #2, replacementMap keys should NOT contain '\n' characters,
    * unless it is the first character (to indicate a string in the beginning of a line)!
    */
  def pipeWithLinesReplacment(src: InputStream,
                              dest: PrintStream,
                              replacementMap: Map[String, String],
                              defaultString: String) {
    logger.debug("map is: " + replacementMap.toString)

    replacementMap.isEmpty match {
      case true => { IOUtils.copy(src, dest) }
      case false => {
        val sb: StringBuilder = new StringBuilder
        val max = replacementMap.foldLeft(0) { case (i, (k, _)) => if (i >= k.length) i else k.length }
        while (true) {
          val ch = src.read.toChar
          if (!mapContainsSuffixOf(replacementMap, sb.toString)) {
            if (ch == '\r') {
              dest.print(sb.toString)
              dest.flush()
              sb.clear
            } else if (ch == '\n') {
              dest.print(sb.toString)
              dest.flush()
              sb.clear
            }
            //                        else if (mapContainsPrefixOf(replacementMap, sb.toString)){
            //                            val prefix = getUnmatchedToAnyKeyPrefix(replacementMap, sb.toString)
            //                            //TODO: implement the above method calls to enable better UX...
            //
            //                        }
            else if (sb.length == max) {
              val tl = sb.toString.tail
              val hd = sb.toString.head
              sb.clear
              sb.appendAll(tl)
              dest.print(hd)
              dest.flush()
            }
          } else {
            val key = getKeyThatIsSuffixOf(replacementMap, sb.toString)
            val value = replacementMap(key)
            val toPrint = sb.toString.dropRight(key.length) ++ value
            sb.clear
            dest.print(toPrint)
            dest.flush()
          }
          sb.append(ch)
        }
      }
    }

    /*
    //val writer = new java.io.BufferedWriter(new java.io.OutputStreamWriter(dest))
    val reader = new java.io.BufferedReader(new java.io.InputStreamReader(src))
    @tailrec
    def transfer(reader: java.io.BufferedReader, writer: java.io.PrintStream) {
        val outputString = reader.readLine match {
            case s: String if (replacementMap(s) != defaultString) => {
                logger.debug("transfering...\n\tgot: " + s + "\n\twill put: " + replacementMap(s))
                replacementMap(s)
            }
            case s: String => s
            case null => { /*TODO: needs to hold back for a while until more input is recieved ; */ "" }
        }

        writer.print(outputString)
        writer.flush
        transfer(reader, writer)
    }

    transfer(reader, dest)*/

  }

  /**
    * kill a process represented by a pid
    */
  def kill(pid: Int) { kill(pid.toString) }
  def kill(pid: String) {
    val seq = if (isWin) {
      Seq("taskkill", "/f", "/t", "/pid", pid)
    } else {
      Seq("kill", pid)
    }

    Process(seq) !;
  }

  def kill9(pid: Int) { kill9(pid.toString) }
  def kill9(pid: String) {
    val seq = if (isWin) {
      Seq("taskkill", "/f", "/t", "/pid", pid)
    } else {
      Seq("kill", "-9", pid)
    }

    Process(seq) !;
  }

  /**
    * returns an empty Set when no such process is found,
    * or a Set of all suitable pids
    */
  def getPid(name: String): Set[Int] = {
    val jpsResult = jps.filter(p => p._2(0).contains(name))
    if (jpsResult.isEmpty) {
      Set[Int]()
    } else {
      for {
        p <- jpsResult
      } yield p._1
    }
  }

  def killWaitAndKill9(pid: Int, ttl: Int): Unit = killWaitAndKill9(pid.toString, ttl)
  def killWaitAndKill9(pid: String, ttl: Int): Unit = {
    kill(pid)
    var ms = 0
    while (!jps.filter(p => p._1.toString == pid).isEmpty) {
      Thread.sleep(500)
      ms += 500
      if (ms >= ttl) kill9(pid)
    }
  }

  /**
    * return value is a set of pairs: (pid, process info),
    * where process info is either unavailable, or a list of:
    * 1- main class
    * 2- main args
    * 3- jvm args
    * 4- jvm flags
    */
  def jps: Set[(Int, List[String])] = {
    val arguments: Arguments = new Arguments(Array[String]("-l", "-v", "-V", "-m"))
    val hostId: HostIdentifier = arguments.hostId
    val monitoredHost: MonitoredHost = MonitoredHost.getMonitoredHost(hostId)
    val jvms = scala.collection.immutable.Set[Int]() ++ (for { j <- monitoredHost.activeVms.asScala } yield j.toInt)
    for {
      lvmid <- jvms
      vm: MonitoredVm = monitoredHost.getMonitoredVm(new VmIdentifier("//" + lvmid + "?mode=r"), 0)
    } yield
      (lvmid, if (vm == null) {
        " -- process information unavailable" :: Nil
      } else {
        val r = MonitoredVmUtil.mainClass(vm, arguments.showLongPaths) ::
          MonitoredVmUtil.mainArgs(vm) ::
          MonitoredVmUtil.jvmArgs(vm) ::
          MonitoredVmUtil.jvmFlags(vm) :: Nil
        monitoredHost.detach(vm)
        r
      })
  }

  /**
    * Run a ProcessBuilder, collecting the stdout, stderr and exit status
    * and based on that, decide what to do next
    */
  def runAndDo(pb: ProcessBuilder)(runAgain: (List[String], List[String]) => Boolean, doAction: => Any): Int = {

    var out = List[String]()
    var err = List[String]()

    val exit = pb ! ProcessLogger((s) => out ::= s, (s) => err ::= s)

    if (runAgain(out.reverse, err.reverse)) {
      doAction
      runAndDo(pb)(runAgain, doAction)
    } else exit
  }
}
*/
