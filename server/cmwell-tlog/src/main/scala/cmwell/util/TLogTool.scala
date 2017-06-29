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


package cmwell.util

import com.typesafe.scalalogging.LazyLogging
import org.rogach.scallop.{Subcommand, ScallopConf}

import cmwell.tlog.TLog
import scala.collection.mutable
import cmwell.common._
import cmwell.common.WriteCommand
import cmwell.common.BulkCommand
import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import cmwell.domain.{FieldValue, Infoton}
import org.joda.time.DateTime

/**
 * Created with IntelliJ IDEA.
 * User: markz
 * Date: 7/4/13
 * Time: 12:41 PM
 * To change this template use File | Settings | File Templates.
 */


/*
* java -jar xxx path name partition --all ]
*
* */
object TLogTool extends App with LazyLogging {

  var commandFilterInclude : Set[String] = Set("all")
  var dataFilterInclude : Set[String] = Set("all")

  class Conf(arguments: Array[String]) extends ScallopConf(arguments) {

    private[this] def isFile(s: List[String]) : Boolean = {
      if ( s.size != 3 )
        false
      else {
        //val fileName = s(0) + s(1) + "_" + s(2) + ".dat"

        //val b = (new File(fileName)).isFile
        //if ( !b ) println("$fileName does not exists.")
        //b
        true
      }
    }

    version("tlcat 1.0.0 (c) 2014 CMWELL")
    banner("""Usage: java -jar cmwell-tlog-ng_2.10-1.2.1-SNAPSHOT-selfexec.jar [OPTION]... [PATH] [NAME] [PARTITION]
             |tlcat is tlog for h
             |Options:
             |""".stripMargin)
    footer("\nFor bugs call dudi!")

    val cmdWrite  = opt[Boolean]("w" , descr = "extract WriteCommand")
    val cmdOWrite = opt[Boolean]("o" , descr = "extract WriteCommand")
    val cmdUpdate = opt[Boolean]("u" , descr = "extract UpdateCommand")
    val cmdDelete = opt[Boolean]("d" , descr = "extract all DeleteCommands")

    val offset    = opt[Long]("s" , default = Some(0L), descr = "read offset")

    /*
    val dataPath = opt[Boolean]("path" , descr = "extract path ")
    val dataParent = opt[Boolean]("parent" , descr = "extract parent ")
    val dataLastMotified = opt[Boolean]("lm" , descr = "extract path ")
    val dataUUid = opt[Boolean]("uuid" , descr = "extract path ")
    val dataWeight = opt[Boolean]("weight" , descr = "extract path ")
    val dataFields = opt[Boolean]("fields", descr = "extract path ")
    */

    val tlogFile = trailArg[List[String]]("tlogFile", required = true, descr = "tlog full name and path", validate = isFile)
    verify()
  }
  val conf = new Conf(args)

  // build the include list for the commands
  if ( conf.cmdWrite() || conf.cmdUpdate() || conf.cmdDelete() ) {
    commandFilterInclude = Set.empty[String]
    if ( conf.cmdWrite() )
      commandFilterInclude = commandFilterInclude + "w"
    if ( conf.cmdUpdate() )
      commandFilterInclude = commandFilterInclude + "u"
    if ( conf.cmdDelete() )
      commandFilterInclude = commandFilterInclude + "d"
    if ( conf.cmdOWrite() )
      commandFilterInclude = commandFilterInclude + "o"
  }
  val l = conf.tlogFile()
  println(l)
  extract(l(1),l(2),l(0).dropRight(11), conf.offset.get.get)


  def printUpdateCommandData(path : String, deleteFields:Map[ String,Set[FieldValue]], updateFields:Map[ String,Set[FieldValue]]  ,lastModified:DateTime) {
    val sb : mutable.StringBuilder = new mutable.StringBuilder()
    sb.append(s"UpdatePathCommand| path [${path}]")
    sb.append("|")
    sb.append(s"modified date [${lastModified}]")
    sb.append("|")
    sb.append(deleteFields.mkString("[",",","]"))
    sb.append("|")
    sb.append(updateFields.mkString("[",",","]"))
    sb.append("|")
    println(sb.toString())
  }

  def printWriteCommandData(i : Infoton, isOW: Boolean = false) {
    var allData : Boolean = dataFilterInclude.contains("all")
    val sb : mutable.StringBuilder = new mutable.StringBuilder()
    sb.append(if(isOW) "OverwriteCommand" else "WriteCommand|")
    if ( allData || dataFilterInclude.contains("path") ) {
      sb.append(s"path [${i.path}]")
      sb.append("|")
    }
    if ( allData || dataFilterInclude.contains("parent") ) {
      sb.append(s"parent [${i.parent}]")
      sb.append("|")
    }
    if ( allData || dataFilterInclude.contains("dc") ) {
      sb.append(s"dc [${i.dc}]")
      sb.append("|")
    }
    if ( allData || dataFilterInclude.contains("lm") ) {
      sb.append(s"modified date [${i.lastModified}]")
      sb.append("|")
    }
    if ( allData || dataFilterInclude.contains("uuid")) {
      sb.append(s"uuid [${i.uuid}]")
      sb.append("|")
    }
    if ( allData || dataFilterInclude.contains("weight")) {
      sb.append(s"weight [${i.weight}]")
      sb.append("|")
    }
    if(isOW){
      sb.append(i.indexTime.getOrElse(-1L))
      sb.append("|")
    }
    if ( allData || dataFilterInclude.contains("fields")) {
      i.fields match {
        case Some(f) =>
          sb.append(s"fields [${f}]")
        case None =>
          sb.append(s"fields [None]")
      }
      sb.append("|")
    }
    println(sb.toString())
  }


  def commands_extractor(cmd : Command) {
    val allCommands : Boolean = commandFilterInclude.contains("all")
    cmd match {
      case WriteCommand(i,trackingID,prevUUID) =>
        if ( allCommands || commandFilterInclude.contains("w") )
          printWriteCommandData(i)
      case OverwriteCommand(i,trackingID) =>
        if ( allCommands || commandFilterInclude.contains("o") )
          printWriteCommandData(i,true)
      case UpdatePathCommand(path, deleteFields, updateFields,lastModified,trackingID,prevUUID) =>
        if ( allCommands || commandFilterInclude.contains("u") )
          printUpdateCommandData(path, deleteFields, updateFields,lastModified)
      case DeletePathCommand(path,lm,trackingID,prevUUID) =>
        if ( allCommands || commandFilterInclude.contains("d") )
          println(s"DeletePathCommand|${path}|${lm}")
      case DeleteAttributesCommand(path,fields,lm,trackingID,prevUUID) =>
        if ( allCommands || commandFilterInclude.contains("d") )
          println(s"DeleteAttributesCommand|${path}|${fields}|${lm}")
      case MergedInfotonCommand(prev,current,_) =>
        if ( allCommands || commandFilterInclude.contains("m") )
          println(s"MergedInfotonCommand|$prev|$current")
      case OverwrittenInfotonsCommand(previous,current,historic) =>
        if ( allCommands || commandFilterInclude.contains("o") )
          println(s"OverwrittenInfotonsCommand|$previous|$current|${historic.mkString("[",",","]")}")
      case BulkCommand(cmds) =>
        cmds.foreach  {
          i => commands_extractor(i)
        }
      case _ => ???
    }
  }

  def extract(tlog_name : String , tloag_partition_name : String , cmwell_home_path : String , offset : Long ) {
    // lets open TLog file
    val tlog : TLog = TLog(tlog_name , tloag_partition_name , cmwell_home_path)
    tlog.init()
    var ts : Long = offset - 1
    var next_ts : Long = offset
    while ( ts != next_ts ) {
      ts = next_ts
      val data = tlog.read(ts,1)
      next_ts = data._1
      val vec = data._2
      vec.foreach{
        item =>
          val cmd : Command = CommandSerializer.decode(item._2)
          commands_extractor(cmd)
      }
    }
    tlog.shutdown()
  }

}
