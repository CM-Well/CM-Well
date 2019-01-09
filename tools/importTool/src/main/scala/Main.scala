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
import akka.actor.{ActorSystem, Props}
import org.rogach.scallop.ScallopConf


  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val sourceUrl = opt[String]("source-url", required = true, descr = "the source url which download rdf file")
    val format = opt[String]("format", required = true, descr="the ofile format")
    var cluster = opt[String]("cluster", required = true, descr="the target server which content is ingested to")
    verify()
  }

  object Main {
    def main(args: Array[String]) {
      val conf = new Conf(args)  // Note: This line also works for "object Main extends App"
      println("source file is: " + conf.sourceUrl())
      println("output format is: " + conf.format())
      val system = ActorSystem("MySystem")
      println("About to Start import tool flow...")
      val mainActor = system.actorOf(Props(new AkkaFileReaderWithActor(conf.sourceUrl(), conf.format(), conf.cluster())), name = "myactor")
      mainActor ! ActorInput
    }

}
