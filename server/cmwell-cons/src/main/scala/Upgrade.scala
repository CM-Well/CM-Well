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

import semverfi.{SemVersion, Version}

import scala.collection.GenSeq
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Upgrade {

  case class UpgradeFunc (applyToVersion: String, desc: String, func: (GenSeq[String]) => Future[Boolean]){
    def executeUpgrade(hosts: GenSeq[String]) = {
      if (Await.result(func(hosts), 1.minutes))
        info(s"$desc succeeded")
      else
        error(s"$desc failed")
    }
  }

  val postUpgradeList : List[UpgradeFunc] = {
    //example
    /*List(UpgradeFunc("1.6.2", "myUpgradeSomething",(hosts: GenSeq[String]) => {info(s"in func print: ${hosts(0)}"); Future.successful(true)}),
      UpgradeFunc("1.6.7", "myUpgradeSomethingFailed",(hosts: GenSeq[String]) => {info(s"in func print failed: ${hosts(0)}"); Future.successful(false)}))*/
    Nil
  }

  def runPostUpgradeActions(currentVersionF : Future[String], upgraded : String, hosts: GenSeq[String]): Future[Boolean]= {
    def shouldRun(test : SemVersion, curr : SemVersion, upgrade : SemVersion) : Boolean = (test > curr) && (test <= upgrade)

    currentVersionF.foreach{current =>
      val currentVersion = Version(current)
      val upgradedVersion = Version(upgraded)

      val relevantUpgradeFunctions = postUpgradeList.filter(f => shouldRun(Version(f.applyToVersion), currentVersion, upgradedVersion))
      info(s"Going to run ${relevantUpgradeFunctions.size} upgrade functions")

      relevantUpgradeFunctions.sortBy(f => Version(f.applyToVersion)).foreach(_.executeUpgrade(hosts))
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

