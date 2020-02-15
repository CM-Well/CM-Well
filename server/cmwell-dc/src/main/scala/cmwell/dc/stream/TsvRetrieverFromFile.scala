/**
  * © 2019 Refinitiv. All Rights Reserved.
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
package cmwell.dc.stream

import java.io.{BufferedWriter, File, FileWriter}

import akka.actor.ActorSystem
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import akka.stream.Supervision.Decider
import akka.stream.contrib.SourceGen
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import cmwell.dc.LazyLogging
import cmwell.dc.stream.MessagesTypesAndExceptions.{DcInfo, InfotonData}
import cmwell.dc.stream.TsvRetriever.{logger, TsvFlowOutput}
import cmwell.util.resource._

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by eli on 20/12/16.
  */
object TsvRetrieverFromFile extends LazyLogging {

  def apply(dcInfo: DcInfo)(implicit mat: Materializer,
                            system: ActorSystem): Source[InfotonData, (KillSwitch, Future[Seq[Option[String]]])] = {
    val persistFile = dcInfo.tsvFile.get + ".persist"

    def appendToPersistFile(str: String): Unit = {
      val bw = new BufferedWriter(new FileWriter(persistFile, true))
      bw.write(str)
      bw.close()
    }

    val linesToDrop = dcInfo.positionKey.fold {
      if (!new File(persistFile).exists) 0L
      else using(scala.io.Source.fromFile(persistFile))(_.getLines.toList.last.toLong)
    }(pos => pos.toLong)
    val positionKeySink = Flow[InfotonData]
      .recover {
        case e: Throwable => InfotonData(null, null, -1)
      }
      .scan(linesToDrop) {
        case (count, InfotonData(null, null, -1)) => {
          appendToPersistFile("crash at: " + count + "\n" + count.toString + "\n")
          count
        }
        case (count, _) => {
          val newCount = count + 1
          if (newCount % 10000 == 0) appendToPersistFile(newCount.toString + "\n")
          newCount
        }
      }
      .toMat(Sink.last)(
        (_, right) =>
          right.map { count =>
            appendToPersistFile(count.toString + "\n")
            Seq.fill(2)(Option(count.toString))
        }
      )

    Source
      .fromIterator(() => scala.io.Source.fromFile(dcInfo.tsvFile.get).getLines())
      .drop {
        logger.info(s"Dropping $linesToDrop initial lines from file ${dcInfo.tsvFile.get} for sync ${dcInfo.key}")
        linesToDrop
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .map(line => TsvRetriever.parseTSVAndCreateInfotonDataFromIt(ByteString(line)))
      .alsoToMat(positionKeySink)(Keep.both)
  }
}
