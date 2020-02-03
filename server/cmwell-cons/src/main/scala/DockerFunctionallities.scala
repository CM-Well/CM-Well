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
import scala.util.{Failure, Success, Try}
import scala.sys.process._

/**
  * Created by michael on 2/5/15.
  */
trait DockerFunctionallities {

  private def command(com: String): Try[String] = {
    val seq = Seq("bash", "-c", com)
    Try(seq.!!)
  }

  def getDockerIps: Seq[String] = {
    command(
      """docker ps | grep cmwellimage | awk '{print $1}' | xargs -I zzz docker inspect zzz | grep IPAddress | awk -F '"' '{print $4}'"""
    ) match {
      case Success(str) =>
        val res = str.trim.split("\n")
        res.toSeq
      case Failure(err) => Seq.empty
    }
  }

  def spawnDockerNode(casDir: Option[String] = None,
                      cclDir: Option[String] = None,
                      esDir: Option[String] = None,
                      tlogDir: Option[String] = None,
                      logsDir: Option[String] = None,
                      instDir: Option[String] = None) {
    val casDirCom = casDir.map(str => if (str != "") s"-v $str:/mnt/d1/cas" else "").getOrElse("")
    val cclDirCom = cclDir.map(str => if (str != "") s"-v $str:/mnt/d1/ccl" else "").getOrElse("")
    val esDirCom = esDir.map(str => if (str != "") s"-v $str:/mnt/d2/es" else "").getOrElse("")
    val tlogDirCom = tlogDir.map(str => if (str != "") s"-v $str:/mnt/d2/tlog" else "").getOrElse("")
    val logsDirCom = logsDir.map(str => if (str != "") s"-v $str:/mnt/d2/log" else "").getOrElse("")
    val instDirCom = instDir.map(str => if (str != "") s"-v $str:/mnt/s1/cmwell-installation" else "").getOrElse("")

    command(s"""docker run -d -P $casDirCom $cclDirCom $esDirCom $tlogDirCom $logsDirCom $instDirCom cmwellimage""")
  }

  def killCmwellDockers {
    command("""docker ps | grep cmwellimage | awk '{print $1}' | xargs -I zzz docker rm -f zzz""")
  }
}
