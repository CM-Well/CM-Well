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
package cmwell.util.connection

import com.jcraft.jsch._
import java.io.{InputStream, OutputStream}
import com.typesafe.scalalogging.LazyLogging

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 5/29/13
  * Time: 2:44 PM
  * To change this template use File | Settings | File Templates.
  */
package object ssh extends {

  class CustomJschUserInfo(psswrd: String, yesNoController: String => Boolean)
      extends UserInfo
      with LazyLogging {

    //keep this default yesNoController ? (for testing, and such...)
    private def myPrompt: Boolean = {
      scala.io.StdIn.readLine().toLowerCase match {
        case "y"   => true
        case "yes" => true
        case "no"  => false
        case "n"   => false
        case _     => throw new IllegalArgumentException("YES or NO only!!!")
      }
    }

    def getPassphrase(): String = { null }

    def getPassword(): String = { psswrd }

    def promptPassword(msg: String): Boolean = yesNoController(msg)

    def promptPassphrase(msg: String): Boolean = yesNoController(msg)

    def promptYesNo(msg: String): Boolean = yesNoController(msg)

    def showMessage(msg: String): Unit = { logger.info(msg) }
  }

  class SshConnectionProblem(msg: String)
      extends cmwell.util.exceptions.ConnectionException(msg)

  trait SshExecutor {
    def getExecChannel(command: String): ChannelExec

    def getSftpChannel: ChannelSftp

    def getHost: String

    def getUserName: String

    def disconnect: Unit
  }

  /**
   * use SSH to log onto <i>host</i> as <i>user</i> using <i>password</i> to execute a <i>command</i>
   * @param user the remote host's user
   * @param host the host to connect to
   * @param password the password for user@host
   * @param yesNoController [OPTIONAL] in case you want to control yes/no prompt (default behavior is auto approval - yes to all)
   * @return a tuple with InputStream & OutputStream connected to the remote execution
   */
  def createSshExecutor(
    user: String,
    host: String,
    password: String,
    yesNoController: String => Boolean = _ => true): SshExecutor = {

    class SshExecutorImpl(val session: Session)
        extends SshExecutor
        with LazyLogging {

      private lazy val gotConnected = {
        val config = new java.util.Properties()
        config.put("StrictHostKeyChecking", "no")
        session.setConfig(config)
        if (!session.isConnected) {
          try {
            session.connect()
            true
          } catch {
            case e: Exception =>
              logger.error(
                "caught exception: " + e.getMessage + "\n with stacktrace:" + e.getStackTrace
                  .mkString("\n\t", "\n\t", "")
              ); false
          }
        } else true
      }

      def getHost = session.getHost

      def getUserName = session.getUserName

      def getExecChannel(command: String): ChannelExec = {
        if (gotConnected && session.isConnected) {
          val channel: ChannelExec =
            session.openChannel("exec").asInstanceOf[ChannelExec]
          channel.setCommand(command)
          channel
        } else throw new SshConnectionProblem("could not open a connection!")
      }

      def getSftpChannel: ChannelSftp = {
        if (gotConnected && session.isConnected) {
          session.openChannel("sftp").asInstanceOf[ChannelSftp]
        } else throw new SshConnectionProblem("could not open a connection!")
      }

      def disconnect { session.disconnect }
    }

    val jsch = new JSch
    val session = jsch.getSession(user, host, 22)
    val ui = new CustomJschUserInfo(password, yesNoController)
    session.setUserInfo(ui)
    new SshExecutorImpl(session)
  }

  def sshExec(ssh: SshExecutor, cmd: String): List[String] = {
    val ce = ssh.getExecChannel(cmd)
    ce.setInputStream(null)
    val in = ce.getInputStream
    ce.connect
    val output = scala.io.Source.fromInputStream(in).getLines().toList
    //if(output.contains(expectedOutput)) logger.info("command: " + cmd + " succeeded!")
    //else {logger.error("something is wrong on: " + machine + ", command: " + cmd + " did not succeed.") }
    ce.disconnect
    output
  }
}
