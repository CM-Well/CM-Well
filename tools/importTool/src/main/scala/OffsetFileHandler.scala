import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout

import scala.io.Source


class OffsetFileHandler {


  def persistOffset(lastSuccessfulOffset:Int) = {

    // FileWriter
    val file = new File("lastOffset")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(lastSuccessfulOffset.toString)
    bw.close()

  }

  def readOffset = {

    Source.fromFile("./lastOffset").getLines.toList.head.toInt
  }

}