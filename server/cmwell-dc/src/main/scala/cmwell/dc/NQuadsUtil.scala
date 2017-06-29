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


package cmwell.dc

/**
 * Created by gilad on 10/8/15.
 */
trait NQuadsUtil extends cmwell.dc.LazyLogging {

  def fakeIndexTimeFromLastModifiedIfExists(nquads: Seq[String]): Seq[String] = {
    val groupedBySubject = nquads.groupBy(_.trim.split(' ')(0))
    groupedBySubject.toSeq.flatMap{
      case (subject,lines) => {
        if(lines.exists(_.contains("""/meta/sys#indexTime>"""))) lines
        else {
          logger.trace(s"indexTime not found for: ${subject}, going to fake it (nquads.size=${nquads.size}})")
          val stmtOpt = lines.find(_.contains("""/meta/sys#lastModified>"""))
          val missingIndexTimeOpt = stmtOpt.flatMap{ stmt =>
            val splitted = stmt.trim.split(' ')
            val typedDateValue = splitted(2).trim
            val dateAsString = typedDateValue.tail.takeWhile(_ != '\"')
            cmwell.util.string.parseDate(dateAsString).map{ fakeDate =>
              val fakeIndexTime = fakeDate.getMillis
              val indexTimeAttribute = splitted(1).replace("lastModified","indexTime")
              val newStmt = subject + " " + indexTimeAttribute + " " + "\"" + fakeIndexTime.toString + "\"^^<http://www.w3.org/2001/XMLSchema#long> ."
              newStmt
            }
          }
          missingIndexTimeOpt ++: lines
        }
      }
    }
  }

  def groupBySubject(nquads: Seq[String]): Map[Option[String],Seq[String]] = {
    nquads.groupBy { line =>
      val wrappedSubject = line.trim.split(' ')(0) //i.e: `< sub_url >`
      if(wrappedSubject.size > 1) Some(wrappedSubject.tail.init)
      else {
        logger.error("bad statement: " + line)
        logger.debug("detailed ERROR message, full malformed nquads \n" + nquads.mkString("\n"))
        None
      }
    }
  }

  def isHavingIndexTimeForAllSubjects(nquads: Seq[String]): Boolean = {
    val groupedBySubject = nquads.groupBy(_.trim.split(' ')(0))
    groupedBySubject.forall{
      case (subject,lines) => lines.exists(_.contains("""/meta/sys#indexTime>"""))
    }
  }
}

trait DCFixer extends NQuadsUtil {

  def dc: String

  lazy val dcReplacement: String = "/meta/sys#dataCenter> \"" + dc + "\" ."

  def fixDcAndIndexTime(nquads: Seq[String]): Seq[String] = {
    if(isHavingIndexTimeForAllSubjects(nquads)) handleMissingDc(nquads)
    else handleMissingDc(fakeIndexTimeFromLastModifiedIfExists(nquads))
  }

  def handleMissingDc(nquads: Seq[String]): Seq[String] = {
    //TODO: naive implementation. needs improvement
    if(!nquads.exists(_.contains("""/meta/sys#dataCenter> "na" ."""))) nquads
    else nquads.map(_.replaceAll("""/meta/sys#dataCenter> "na" .""",dcReplacement))
  }
}
