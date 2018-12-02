import akka.actor.{ActorSystem, Props}
import org.rogach.scallop.ScallopConf


  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val sourceUrl = opt[String]("source-url", required = true, descr = "the source url which download rdf file")
    val format = opt[String]("format", required = true, descr="the ofile format")
    var cluster = opt[String]("cluster", required = true, descr="the target server which content is ingested to")
    val numConn = opt[String]("numConn", required = true, descr="the number of connections to cm-well")
    verify()
  }

  object Main {
    def main(args: Array[String]) {
      val conf = new Conf(args)  // Note: This line also works for "object Main extends App"
      println("source file is: " + conf.sourceUrl())
      println("output format is: " + conf.format())
      val system = ActorSystem("MySystem")
      println("About to Start import tool flow...")
      val mainActor = system.actorOf(Props(new AkkaFileReaderWithActor(conf.sourceUrl(), conf.format(), conf.cluster(), conf.numConn().toInt)), name = "myactor")
      mainActor ! ActorInput
    }

}
