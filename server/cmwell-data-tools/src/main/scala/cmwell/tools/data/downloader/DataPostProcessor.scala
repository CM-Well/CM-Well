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


package cmwell.tools.data.downloader

import akka.stream.scaladsl._
import akka.util.{ByteString, ByteStringBuilder}
import cmwell.tools.data.utils.akka._
import cmwell.tools.data.utils.chunkers.GroupChunker
import cmwell.tools.data.utils.logging.DataToolsLogging

import scala.collection.mutable

object DataPostProcessor extends DataToolsLogging {

  def postProcessByFormat(format: String, dataBytes: Source[ByteString, _]) = format match {
    case "ntriples" | "nquads" => sortBySubjectOfNTuple(dataBytes, 255)
    case _                     => splitByLines(dataBytes)
  }

  private def splitByLines(dataBytes: Source[ByteString, _]) = {
    dataBytes
      .via(lineSeparatorFrame)
      .map(_ ++ endl)
  }

  private def sortBySubjectOfNTuple(dataBytes: Source[ByteString, _], maxSubjects: Int): Source[ByteString, _] = {
    dataBytes
      .via(lineSeparatorFrame)
      .filter {
        case line if line.startsWith("_") =>
          redLogger.error("was filtered: {}", line.utf8String)
          badDataLogger.error(line.utf8String)
          false
        case _ => true
      }
      .fold(mutable.Map.empty[ByteString, ByteStringBuilder]){ (agg, line) =>
        // aggregate each line according to its subject (i.e., bucket)
        val subject = GroupChunker.extractSubject(line)
        val builder = agg.getOrElse(subject, new ByteStringBuilder)
        builder ++= (line ++ endl)
        agg + (subject -> builder)
      }
      .map(_.toMap)
      .mapConcat(_.map{ case (_, ntupleBuilder) => ntupleBuilder.result})
  }
}

