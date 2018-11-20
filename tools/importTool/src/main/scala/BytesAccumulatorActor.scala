import java.nio.file.{Files, Paths}

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}



case class Message(value: Int)
case object LastOffset

class BytesAccumulatorActor() extends Actor {
  var count = 0

  def receive: Receive = {
    case Message(bytes) =>
      println("Ingest more " + bytes + " successfully")
      count += bytes
      OffsetFileHandler.persistOffset(count)
      println("Ingest total bytes " + count)

    case LastOffset => if(Files.exists(Paths.get("./lastOffset"))) {
      count = OffsetFileHandler.readOffset
      sender ! count
    } else sender ! 0
    case x =>
      println("Not supported action" + x)
  }

}