import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import org.rogach.scallop.ScallopConf
import scala.concurrent.duration._


  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val sourceUrl = opt[String]("source-url", required = true)
    val format = opt[String]("format", required = true)
    verify()
  }

  object Main {
    def main(args: Array[String]) {
      val conf = new Conf(args)  // Note: This line also works for "object Main extends App"
      println("source file is: " + conf.sourceUrl())
      println("output format is: " + conf.format())
      val system = ActorSystem("MySystem")
        implicit val timeout = Timeout(5 seconds)
        println("Hi")
        val myActor = system.actorOf(Props(new AkkaFileReaderWithActor()), name = "myactor")
        myActor ! ActorInput(conf.sourceUrl(), conf.format())
    }

}
