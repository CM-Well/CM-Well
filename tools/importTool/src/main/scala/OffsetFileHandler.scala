import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout

import scala.io.Source


object OffsetFileHandler {


  def persistOffset(lastSuccessfulOffset:Long) = {

    val file = new File("./lastOffset")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(lastSuccessfulOffset.toString)
    bw.close()

  }

  def readOffset = {
    val source = Source.fromFile("./lastOffset")
    try { source.getLines().toList.head.toInt }
    finally { source.close() }
  }

}