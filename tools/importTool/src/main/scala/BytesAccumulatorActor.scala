import java.nio.file.{Files, Paths}

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}



case class Message(msg: Int)

class BytesAccumulatorActor() extends Actor {
  var count = 0
  val offsetFileHandler = new OffsetFileHandler

  def receive: Receive = {
    case Message(bytes) =>
      println("Actor adding bytes=", bytes)
      count += bytes
      offsetFileHandler.persistOffset(count)
      println("Actor counter=" + count)

    case "test" => if(Files.exists(Paths.get("./Write.txt"))) {
      count = offsetFileHandler.readOffset
      sender ! count
    } else sender ! 0
//    case "test" => sender ! count

    case "failed" => println("====================Boom=====================")
    case x =>
      println("bye bye=" + x)
  }

}