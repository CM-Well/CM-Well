package com.poc

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