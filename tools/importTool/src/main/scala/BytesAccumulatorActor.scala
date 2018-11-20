import java.nio.file.{Files, Paths}

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}



case class Message(value: Int)
case object LastOffset
case class FileSize(value:Long)

class BytesAccumulatorActor() extends Actor {
  var count = 0
  var fileSizeInBytes = 0L

  def receive: Receive = {
    case Message(bytes) =>
      count += bytes
      OffsetFileHandler.persistOffset(count)
      println("Progress of ingest infotons: " + (count * 100.0f) / fileSizeInBytes + "%")
    case FileSize(bytes) => fileSizeInBytes = bytes
    case LastOffset => if(Files.exists(Paths.get("./lastOffset"))) {
      count = OffsetFileHandler.readOffset
      sender ! count
    } else sender ! 0
    case x =>
      println("Not supported action" + x)
  }

}