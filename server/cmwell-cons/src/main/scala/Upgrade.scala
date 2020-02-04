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

import nl.gn0s1s.bump.SemVer

import scala.collection.parallel.ParSeq
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Upgrade {

  case class UpgradeFunc (applyToVersion: String, desc: String, func: (ParSeq[String]) => Future[Boolean]){
    def executeUpgrade(hosts: ParSeq[String]) = {
      if (Await.result(func(hosts), 1.minutes))
        info(s"$desc succeeded")
      else
        error(s"$desc failed")
    }
  }

  val postUpgradeList : List[UpgradeFunc] = {
    //example
    /*List(UpgradeFunc("1.6.2", "myUpgradeSomething",(hosts: ParSeq[String]) => {info(s"in func print: ${hosts(0)}"); Future.successful(true)}),
      UpgradeFunc("1.6.7", "myUpgradeSomethingFailed",(hosts: ParSeq[String]) => {info(s"in func print failed: ${hosts(0)}"); Future.successful(false)}))*/
    Nil
  }

  def runPostUpgradeActions(currentVersionF : Future[String], upgraded : String, hosts: ParSeq[String]): Future[Boolean]= {
    /**
      *  Note! According to semantic versions rules, what will be taken into account is only <major>.<minor>.<patch>
      *    all text after + sign will not affect precedence!
      *    Examples:  1.2.5  >  1.2.3
      *       but     1.2.5  ==   1.2.5+167
      */
    def shouldRun(test : SemVer, curr : SemVer, upgrade : SemVer) : Boolean = (test > curr) && (test <= upgrade)

    currentVersionF.foreach{current =>
      val currentVersion = SemVer(current)
      val upgradedVersion = SemVer(upgraded)

      if (currentVersion.isEmpty || upgradedVersion.isEmpty)
          error(s"Failed to parse the versions:\n ${
            currentVersion.fold(s"current: $current")(_ => "")} ${
            upgradedVersion.fold(s"upgraded: $upgraded")(_ => "")
          }. Stoping upgrade process!")
      else
      {
        val relevantUpgradeFunctions = postUpgradeList.filter(f => shouldRun(SemVer(f.applyToVersion).get, currentVersion.get, upgradedVersion.get))
        info(s"Going to run ${relevantUpgradeFunctions.size} upgrade functions")

        relevantUpgradeFunctions.sortBy(f => SemVer(f.applyToVersion)).foreach(_.executeUpgrade(hosts))
      }
    }

    Future(true)
  }



  // scalastyle:off
  def info(msg: String) = println(s"Upgrade Info: $msg")
  // scalastyle:on

  // scalastyle:off
  def error(msg: String) = println(s"Upgrade Error: $msg")
  // scalastyle:on
}

