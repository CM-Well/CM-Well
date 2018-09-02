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
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object Upgrade {

  class UpgradeFunc (desc: String, func: (GenSeq[String]) => Boolean){
    def executeUpgrade(hosts: GenSeq[String]) = if (func(hosts)) info(s"$desc succeeded") else error(s"$desc failed")
  }

  val postUpgradeMap : Map[String, UpgradeFunc] = {
    //example
    /*Map( "1.6.2" -> new UpgradeFunc("myUpgradeSomething",(hosts: GenSeq[String]) => {info(s"in func print: ${hosts(0)}"); true}),
      "1.6.7" -> new UpgradeFunc("myUpgradeSomethingFailed",(hosts: GenSeq[String]) => {info(s"in func print failed: ${hosts(0)}"); false}))*/
  }

  def runPostUpgradeActions(currentVersionF : Future[String], upgraded : String, hosts: GenSeq[String])= {
    def shouldRun(test : SemVersion, curr : SemVersion, upgrade : SemVersion) : Boolean = (test > curr) && (test <= upgrade)

    currentVersionF.map(current => postUpgradeMap.foreach{f =>
      val currentVersion = Version(current)
      val upgradedVersion = Version(upgraded)
      val applyToVersion = Version(f._1)

      if (shouldRun(applyToVersion, currentVersion, upgradedVersion)) f._2.executeUpgrade(hosts) })

  }



  // scalastyle:off
  def info(msg: String) = println(s"Upgrade Info: $msg")
  // scalastyle:on

  // scalastyle:off
  def error(msg: String) = println(s"Upgrade Error: $msg")
  // scalastyle:on
}

