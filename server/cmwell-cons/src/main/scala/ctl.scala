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
//import scala.sys.process.processInternal.File

// comment

import java.io.{File, PrintWriter}
import java.util.Date

import cmwell.ctrl.client.CtrlClient
import cmwell.ctrl.hc.{ActiveNodes, ClusterStatus}
import cmwell.util.http.SimpleHttpClient
import k.grid.{GridConnection, Grid => AkkaGrid}
import play.api.libs.json.{JsValue, Json}

import scala.collection.parallel.{ParMap, ParSeq, ParSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
import scala.util.parsing.json._
import scala.util.{Failure, Success, Try}
import scala.collection.parallel.CollectionConverters._
//todo: make sure that some applications are installed.

trait Info {

  // scalastyle:off
  def info(msg: String) = println(s"Info: $msg")
  // scalastyle:on
}

object ResourceBuilder {
  def getIndexedName(name: String, index: Int): String = {
    index match {
      case 1 => name
      case _ => s"$name$index"
    }
  }

  private def replaceTemplates(text: String, templates: Map[String, String]): String =
    """\{\{([^{}]*)\}\}""".r.replaceSomeIn(text, {
      case scala.util.matching.Regex.Groups(name) => templates.get(name)
    })

  def getResource(path: String, map: Map[String, String]): String = {
    val fileContent = Source.fromFile(path).mkString
    replaceTemplates(fileContent, map)
  }
}

abstract class ModuleLock(checkCount: Int = 50) extends Info {
  val delay = 5

  def name: String

  def com(host: String): Try[String]

  def continueCondition(v: String, waitForModuleFor: Int): Boolean

  private var prevRes = "UNLIKLY RES"

  def fail = {
    // scalastyle:off
    println("failed to check " + name)
    // scalastyle:on
    throw new Exception("failed to check " + name)
  }

  def waitForModule(host: String, waitForModuleFor: Int, tries: Int = checkCount): Boolean = {
    if (tries == 0) {
      fail
      false
    } else {
      val res = com(host)
      res match {
        case Success(v) =>
          if (continueCondition(v.trim, waitForModuleFor)) {
            Thread.sleep(delay * 1000)

            val t = if (prevRes != v.trim) {
              prevRes = v.trim
              if (v.trim.length < 40)
                info(s"    $name in progress (${v.trim})")
              else
                info(s"    $name in progress")
              checkCount
            } else tries - 1
            waitForModule(host, waitForModuleFor, t)
          }
        case Failure(e) =>
          Thread.sleep(delay * 1000)
          waitForModule(host, waitForModuleFor, tries - 1)
      }
      true
    }
  }

  def waitForModuleIndefinitely(host: String, waitForModuleFor: Int = 0): Boolean = {
    val res = com(host)
    res match {
      case Success(v) =>
        if (continueCondition(v.trim, waitForModuleFor)) {
          if (v.trim.length < 40)
            info(s"    $name in progress (${v.trim})")
          else
            info(s"    $name in progress")
          Thread.sleep(delay * 1000)
          waitForModuleIndefinitely(host, waitForModuleFor)
        }
      case Failure(e) =>
        Thread.sleep(delay * 1000)
        waitForModuleIndefinitely(host, waitForModuleFor)
    }
    true
  }
}

case class DataDirs(casDataDirs: Seq[String],
                    casCommitLogDirs: Seq[String],
                    esDataDirs: Seq[String],
                    kafkaDataDirs: Seq[String],
                    zookeeperDataDir: String,
                    logsDataDir: String)

//case class DataDirs(m : Map[String,String])

case class InstDirs(intallationDir: String = "~/cm-well-new/cm-well", globalLocation: String = "/opt")

case class HaProxy(host: String, sitedown: String = "cm-well:8080")

object Host {
  var connectedToAkkaGrid = false

  def createHostsNames(name: String, fromIndex: Int, toIndex: Int): List[String] = {
    val digitNum = toIndex.toString.length
    val range = fromIndex to toIndex
    range.toList.map(index => s"${name}%0${digitNum}d".format(index))
  }

  def ctrl = cmwell.ctrl.client.CtrlClient

  def getIndexTxt(moduleIndex: Int) = {
    if (moduleIndex == 1) "" else s"$moduleIndex"
  }
}

trait OsType

case object Oracle extends OsType

case object Ubuntu extends OsType


abstract class Host(user: String,
                    password: String,
                    hostIps: Seq[String],
                    esSize: Int,
                    casSize:Int,
                    val cn: String,
                    val dc: String,
                    dataDirs: DataDirs,
                    instDirs: InstDirs,
                    wsPerMachine: Int,
                    allocationPlan: ModuleAllocations,
                    useAuthorization: Boolean,
                    deployJava: Boolean,
                    production: Boolean,
                    su: Boolean,
                    ctrlService: Boolean = false,
                    minMembers: Option[Int] = None,
                    haProxy: Option[HaProxy],
                    isDebug: Boolean = false,
                    subjectsInSpAreHttps: Boolean = false,
                    defaultRdfProtocol: String = "http",
                    diskOptimizationStrategy:String,
                    casUseCommitLog:Boolean) {
  val cmwellPropertiesFile = "cmwell.properties"

  var sudoerCredentials: Option[Credentials] = None

  def getUser = user

  def getHostIps = hostIps

  def getDataDirs = dataDirs

  def getInstDirs = instDirs

  def getAllocationPlan = allocationPlan

  def getUseAuthorization = useAuthorization

  def getDeployJava = deployJava

  def getProduction = production

  def getSu = su

  def getCtrlSerice = ctrlService

  def getHaProxy = haProxy

  /*
    var useAuthorization = false
    var deployJava = false
    var production = false
    var devMode = false
   */

  def getMinMembers = minMembers.getOrElse(ips.size / 2 + 1)
  def getCasUseCommitLog = casUseCommitLog

  def getThreesome(list: List[String], host: String): String = {
    val from = list.indexOf(host)
    val to = (from + 3) % list.size
    val threesome = if (from < to)
      list.slice(from, to)
    else
      list.take(to) ++ list.drop(from)
    threesome.mkString(",")
  }

  val esRegPort = 9201
  val esMasterPort = 9200

  def currentDir = command("pwd").get

  def getOs(host: String): OsType = {
    val osStr = command("""cat /etc/*-release""", host, false) match {
      case Success(str) => str.trim
      case Failure(err) => "oracle"
    }

    osStr match {
      case str: String if str.toLowerCase().contains("ubuntu") => Ubuntu
      case str: String if str.toLowerCase().contains("oracle") => Oracle
      case str: String                                         => Oracle
    }
  }

  def cssh = {
    checkProduction
    Future {
      command(s"cssh --username $user ${ips.mkString(" ")}")
    }
  }

  def jstat = {
    ips.par.map { ip =>
      // scalastyle:off
      ip -> command(s"ps aux | grep java | egrep -v 'starter|grep' | awk '{print $$2}' | xargs -I zzz ${getInstDirs.globalLocation}/cm-well/app/java/bin/jstat -gcutil zzz", ip, false).map(_.trim)
      // scalastyle:on
    }.toMap
  }

  def jstat(comp: String) = {
    ips.par.map { ip =>
      // scalastyle:off
      ip -> command(s"ps aux | grep java | egrep -v 'starter|grep' | grep $comp | awk '{print $$2}' | xargs -I zzz ${getInstDirs.globalLocation}/cm-well/app/java/bin/jstat -gcutil zzz", ip, false).map(_.trim)
      // scalastyle:on
    }.toMap
  }

  def jstat(comp: String, ip: String): Unit = {
    // scalastyle:off
    ParMap(ip -> command(s"ps aux | grep java | egrep -v 'starter|grep' | grep $comp | awk '{print $$2}' | xargs -I zzz ${getInstDirs.globalLocation}/cm-well/app/java/bin/jstat -gcutil zzz", ip, false).map(_.trim))
    // scalastyle:on
  }

  private val componentToJmxMapping = Map(
    "ws" -> PortManagers.ws.jmxPortManager.initialPort,
    "bg" -> PortManagers.bg.jmxPortManager.initialPort,
    "ctrl" -> PortManagers.ctrl.jmxPortManager.initialPort,
    "dc" -> PortManagers.dc.jmxPortManager.initialPort
  )

  def jconsole(component: String, dualmonitor: Boolean, host1: String, hosts: String*): Unit =
    jconsole(component, dualmonitor, ParSeq(host1) ++ hosts)

  def jconsole(component: String, dualmonitor: Boolean = false, hosts: ParSeq[String] = ips.to(ParSeq)): Unit = {

    if (!dualmonitor) {

      val com = hosts.map(h => s"$h:${componentToJmxMapping(component)}").mkString(" ")
      info(com)
      Future {
        command(s"jconsole $com")
      }
    } else {
      val (hosts1, hosts2) = hosts.splitAt(hosts.size / 2)
      val com1 = hosts1.map(h => s"$h:${componentToJmxMapping(component)}").mkString(" ")
      val com2 = hosts2.map(h => s"$h:${componentToJmxMapping(component)}").mkString(" ")
      info(com1)
      info(com2)
      Future {
        command(s"jconsole $com1")
      }
      Future {
        command(s"jconsole $com2")
      }
    }

  }

  def dcSync(remoteHost: String, dc: String): Unit = {
    // scalastyle:off
    command(s"""curl -XPOST "http://${ips(0)}:9000/meta/sys/dc/$dc" -H "X-CM-Well-Type:Obj" -H "Content-Type:application/json" --data-binary '{"type":"remote" , "location" : "$remoteHost"  , "id" : "$dc"}'""")
    // scalastyle:on
  }

  def ips = hostIps.toList

  def calculateCpuAmount = {

    if(UtilCommands.isOSX)
      command("sysctl hw.physicalcpu", false).get.split(":")(1).trim.toInt
    else
      command("lscpu", false).get.split('\n').map(_.split(':')).map(a => a(0) -> a(1)).toMap.getOrElse("CPU(s)", "0").trim.toInt

  }

  def getEsSize = esSize

  def createFile(path: String,
                 content: String,
                 hosts: ParSeq[String] = ips.to(ParSeq),
                 sudo: Boolean = false,
                 sudoer: Option[Credentials] = None) {
    hosts.foreach(host => createFile(path, content, host, sudo, sudoer))
  }

  def createFile(path: String,
                 content: String,
                 host: String,
                 sudo: Boolean,
                 sudoer: Option[Credentials]) {
    if (sudo) {
      val tempFile = File.createTempFile("pre-", ".txt")
      val fullPath = tempFile.getAbsolutePath
      val fileName = tempFile.getName
      val writer = new PrintWriter(tempFile)
      writer.write(content)
      writer.close()
      _rsync(fullPath, "~", s"${sudoer.get.name}@$host", sudo = false)
      command(s"""sudo mv $fileName $path""", host, true, sudoer)
      command(s"""sudo chown root:root $path""", host, true, sudoer)
      tempFile.delete()
    }
    else
      command(s"""echo $$'$content' > $path""", host, false)
  }

  private def resolveIndex(index: Int): String = {
    index match {
      case 1 => ""
      case _ => s"${index}"
    }
  }

  val deployment = new Deployment(this)

  //FIXME: was object inside a class, caused this nifty hard to track exception:
  /*
   * java.lang.NoSuchFieldError: LogLevel$module
   *  at Host.LogLevel(ctl.scala:415)
   *  at Main$$anon$1.<init>(scalacmd4970030751979185144.scala:10)
   *  at Main$.main(scalacmd4970030751979185144.scala:1)
   *  at Main.main(scalacmd4970030751979185144.scala)
   *  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   *  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
   *  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   *  at java.lang.reflect.Method.invoke(Method.java:497)
   *  at scala.reflect.internal.util.ScalaClassLoader$$anonfun$run$1.apply(ScalaClassLoader.scala:70)
   *  at scala.reflect.internal.util.ScalaClassLoader$class.asContext(ScalaClassLoader.scala:31)
   *  at scala.reflect.internal.util.ScalaClassLoader$URLClassLoader.asContext(ScalaClassLoader.scala:101)
   *  at scala.reflect.internal.util.ScalaClassLoader$class.run(ScalaClassLoader.scala:70)
   *  at scala.reflect.internal.util.ScalaClassLoader$URLClassLoader.run(ScalaClassLoader.scala:101)
   *  at scala.tools.nsc.CommonRunner$class.run(ObjectRunner.scala:22)
   *  at scala.tools.nsc.ObjectRunner$.run(ObjectRunner.scala:39)
   *  at scala.tools.nsc.CommonRunner$class.runAndCatch(ObjectRunner.scala:29)
   *  at scala.tools.nsc.ObjectRunner$.runAndCatch(ObjectRunner.scala:39)
   *  at scala.tools.nsc.ScriptRunner.scala$tools$nsc$ScriptRunner$$runCompiled(ScriptRunner.scala:175)
   *  at scala.tools.nsc.ScriptRunner$$anonfun$runCommand$1.apply(ScriptRunner.scala:222)
   *  at scala.tools.nsc.ScriptRunner$$anonfun$runCommand$1.apply(ScriptRunner.scala:222)
   *  at scala.tools.nsc.ScriptRunner$$anonfun$withCompiledScript$1$$anonfun$apply$mcZ$sp$1.apply(ScriptRunner.scala:161)
   *  at scala.tools.nsc.ScriptRunner$$anonfun$withCompiledScript$1.apply$mcZ$sp(ScriptRunner.scala:161)
   *  at scala.tools.nsc.ScriptRunner$$anonfun$withCompiledScript$1.apply(ScriptRunner.scala:129)
   *  at scala.tools.nsc.ScriptRunner$$anonfun$withCompiledScript$1.apply(ScriptRunner.scala:129)
   *  at scala.tools.nsc.util.package$.trackingThreads(package.scala:43)
   *  at scala.tools.nsc.util.package$.waitingForThreads(package.scala:27)
   *  at scala.tools.nsc.ScriptRunner.withCompiledScript(ScriptRunner.scala:128)
   *  at scala.tools.nsc.ScriptRunner.runCommand(ScriptRunner.scala:222)
   *  at scala.tools.nsc.MainGenericRunner.run$1(MainGenericRunner.scala:85)
   *  at scala.tools.nsc.MainGenericRunner.process(MainGenericRunner.scala:98)
   *  at scala.tools.nsc.MainGenericRunner$.main(MainGenericRunner.scala:103)
   *  at scala.tools.nsc.MainGenericRunner.main(MainGenericRunner.scala)
   *
   *  changed it to an anon val, which is ugly, but works.
   *  this is a TEMPORARY PATCH!
   *  @michaelirzh : please refactor!!!
   */
  val LogLevel = new {
    def warn = deployment.componentProps.collect { case lc: LoggingComponent => lc }.foreach(lc => lc.LogLevel.warn)

    def error = deployment.componentProps.collect { case lc: LoggingComponent => lc }.foreach(lc => lc.LogLevel.error)

    def info = deployment.componentProps.collect { case lc: LoggingComponent => lc }.foreach(lc => lc.LogLevel.info)

    def debug = deployment.componentProps.collect { case lc: LoggingComponent => lc }.foreach(lc => lc.LogLevel.debug)
  }

  private val jwt = sys.env.getOrElse(
    "PUSER_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwVXNlciIsImV4cCI6NDYzODkwMjQwMDAwMCwicmV2IjoxfQ.j-tJCGnWHbJ-XAUJ1wyHxMlnMaLvO6IO0fKVjsXOzYM"
  )
  private val rootDigest =
    sys.env.getOrElse("ROOT_DIGEST", "$2a$10$MKrHtymBOyfE67dZnbEdeOEB336uOXwYetVU28djINKjUTs2da6Km")
  private val rootDigest2 = sys.env.getOrElse("ROOT_DIGEST2", "199245447fd82dd38f84c000da94cf1d")

  var verbose = false

  var deb = isDebug

  def debug: Boolean = deb

  def debug_=(v: Boolean) = {
    deb = v
    // scalastyle:off
    println("The ports are:\nws: 5010\nbatch: 5009\nctrl: 5011\ncw: 5012\ndc: 5013\nbg: 5014")
    // scalastyle:on
  }

  var doInfo = true

  // scalastyle:off
  def info(msg: String) = if (doInfo) println(s"Info: $msg")
  // scalastyle:on

  def warn(msg: String) = {
    // scalastyle:off
    println(s"Warning: $msg")
    // scalastyle:on
  }

  def warnPrompt = {
    // scalastyle:off
    println("Warning: Are you sure you want to continue: (yes/no)")
    // scalastyle:on
    val ln = scala.io.StdIn.readLine()
    if (ln != "yes") {
      throw new Exception("You chose to not continue the process.")
    }
  }

  def getMode: String

  def getCassandraHostIDs(host: String): String

  def getElasticsearchMasters: Int

  def isSu = su

  // scalastyle:off
  def help = println(Source.fromFile("readme").mkString)
  // scalastyle:on

  //def hosts = ips.map(ip => s"${user}@${ip}")
  def getSeedNodes: List[String]

  def javaPath = s"${instDirs.globalLocation}/cm-well/app/java/bin"

  def utilsPath = s"${instDirs.globalLocation}/cm-well/bin/utils"

  def homeBinPath = "~/bin"

  def path: String = s"$javaPath:$utilsPath:$homeBinPath:$$PATH"

  private def ipsToSsh(u: String = user, ips: ParSeq[String]) =
    ips.map(ip => if (ip.indexOf("@") == -1) s"${u}@${ip}" else ip)

  private def timeStamp = System.currentTimeMillis / 1000

  private var lastProdCheckTimeStamp = 0L

  private def checkProduction {
    val interval = 60 * 60

    if (production && (timeStamp - lastProdCheckTimeStamp > interval)) {
      // scalastyle:off
      println("This is a production cluster. Are you sure you want to do this operation: (yes/no)")
      // scalastyle:on
      val ln = scala.io.StdIn.readLine()

      if (ln != "yes") {
        throw new Exception(
          "This operation is not allowed on a production environment. Please remove: production = true from this cluster's definition file"
        )
      } else {
        lastProdCheckTimeStamp = timeStamp
      }
    }
  }

  //  var intallationDir = "~/cm-well-new/cm-well"
  //  var globalLocation = "/opt"

  case class Credentials(name: String, pass: String)

  def gainTrust: Unit = gainTrust()

  def gainTrust(u: String = user, p: String = "", hosts: ParSeq[String] = ips.to(ParSeq)) {
    val sshLocation = s"${sys.env("HOME")}/.ssh"
    val privateKey = s"$sshLocation/id_rsa"
    val publicKey = s"$privateKey.pub"
    val hasPrivateKey = new java.io.File(privateKey).exists
    val hasPublicKey = new java.io.File(publicKey).exists
    val hasKey = hasPrivateKey && hasPublicKey
    if (!hasKey) {
      info("  key not found, generating install key.")
      //ssh-keygen asks for overwrite in case the private key exists, so deleting before.
      Seq("bash", "-c", s"rm -f $privateKey; ssh-keygen -q -t rsa -b 4096 -N '' -C '' -f $privateKey").!!
    }
    val pass = if (p != "") p else scala.io.StdIn.readLine(s"Please enter password for $u\n")
    val sshHosts = ipsToSsh(u, hosts)
    sshHosts.seq.foreach { sshHost =>
      Seq("ssh-keygen", "-R", sshHost).!!
    }
    sshHosts.foreach { sshHost =>
      val cmd = Seq("bash", "-c", s"read PASS; ${UtilCommands.sshpass} -p $$PASS ssh-copy-id -i $privateKey -o StrictHostKeyChecking=no $sshHost")
      // scalastyle:off
      if (verbose) println("command: " + cmd.mkString(" "))
      // scalastyle:on
      (Seq("bash", "-c", s"echo -e -n $pass\\\\n") #| cmd).!!
    }
  }

  def refreshUserState(user: String, sudoer: Option[Credentials], hosts: ParSeq[String] = ips.to(ParSeq)): Unit = {
    // temp disabled for OSX till new cons available...
    val pubKeyOpt = sys.env.get("SSH_DEV_KEY")
    if (!UtilCommands.isOSX && pubKeyOpt.isDefined) {
      val pubKey = pubKeyOpt.get
      val userSshDir = s"/home/$user/.ssh"
      val rootSshDir = "/root/.ssh"
      val fileName = "authorized_keys"
      val rootVarMap = Map("STR" -> pubKey, "DIR" -> rootSshDir, "FILE" -> fileName)
      // scalastyle:off
      val cmdTemplate = "%smkdir -p $DIR; %ssed -i -e '\\$a\\' $DIR/$FILE 2> /dev/null; %sgrep -q '$STR' $DIR/$FILE 2> /dev/null || echo -e '$STR' | %stee -a $DIR/$FILE > /dev/null"
      // scalastyle:on
      val rootCmd = cmdTemplate.format(Seq.fill(4)("sudo "): _*)
      val userCmd = cmdTemplate.format(Seq.fill(4)(""): _*)
      sudoer.foreach(_ => command(rootCmd, hosts, sudo = true, sudoer, rootVarMap))
      val userVarMap = Map("STR" -> pubKey, "DIR" -> userSshDir, "FILE" -> fileName)
      command(userCmd, hosts, sudo = false, sudoer = None, variables = userVarMap)
      //add the file that removes the annoying ssh log in message
      command("touch ~/.hushlogin", hosts, sudo = false)
    }
  }

  //def gainTrustNoPass(u : String = user , p : String = "", hosts : ParSeq[String] = ips.par)

  def validateNumberOfMasterNodes(num: Int, size: Int): Boolean = (num % 2) == 1 && num <= size && num >= 3

  def absPath(path: String) = Seq("bash", "-c", s"cd ${path}; pwd").!!.replace("\n", "")

  val nodeToolLocation = s"${instDirs.globalLocation}/cm-well/app/cas/cur/bin/nodetool"

  def nodeToolPath = nodeToolLocation

  def pingAddress = ips(0)

  def esHealthAddress = ":9200/_cluster/health?pretty=true"

  def cassandraStatus(host: String): Try[String] = {
    command(
      s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath status 2> /dev/null | grep UN | wc -l",
      host,
      false
    ).map(_.trim)
  }

  case class CassandraLock() extends ModuleLock {
    def name: String = "Cassandra boot"

    def com(host: String): Try[String] = cassandraStatus(host)

    def continueCondition(v: String, waitFor: Int): Boolean = v.toInt < waitFor
  }

  case class CassandraDNLock() extends ModuleLock {
    def name: String = "CassandraDownNodes counter"

    def com(host: String): Try[String] =
      command(
        s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath status 2> /dev/null | grep DN | wc -l",
        host,
        false
      )

    def continueCondition(v: String, waitFor: Int): Boolean = v.toInt < waitFor
  }

  case class ElasticsearchLock(checkHost: String = ips(0)) extends ModuleLock {
    def name: String = "Elasticsearch boot"

    def com(host: String): Try[String] = {
      val r = command("curl -sX GET http://" + checkHost + esHealthAddress, host, false)
      r match {
        case Success(v) =>
          Try(JSON.parseFull(v.trim).get.asInstanceOf[Map[String, Any]]("number_of_nodes").toString.trim.split('.')(0))
        case Failure(e) => Failure(e)
      }
    }

    def continueCondition(v: String, waitFor: Int): Boolean = v.trim.toInt < waitFor
  }

  case class ElasticsearchStatusLock(colors: String*) extends ModuleLock {
    override val delay = 20

    def name: String = s"Waiting for Elasticsearch ${colors.mkString(", ")} status"

    def com(host: String): Try[String] = elasticsearchStatus(host)

    def continueCondition(v: String, waitFor: Int): Boolean = !colors.contains(v.toLowerCase)
  }

  case class NoKnownHostsLock() extends ModuleLock {
    override def name: String = "Updating known hosts"

    override def continueCondition(v: String, waitForModuleFor: Int): Boolean = v.contains("known-cmwell-hosts")

    override def com(host: String): Try[String] =
      command("curl -sX GET http://" + host + ":9000/meta/sys?format=ntriples")
  }

  def webServerStatus(host: String) = command("curl -Is http://" + host + ":9000/")

  case class WebServiceLock() extends ModuleLock {
    def name: String = "Web Service boot"

    def com(host: String): Try[String] = webServerStatus(host)

    def continueCondition(v: String, waitFor: Int): Boolean = {
      val containsCmwellHeader = v.contains("X-CMWELL-Hostname:")
      val first = v.lineStream.head
      !(containsCmwellHeader && (first.contains("200") || first.contains("404") || first.contains("503")))
    }
  }

  val dataInitializer = new DataInitializer(this, jwt, rootDigest, rootDigest2)

  def shutDownDataInitializer() = dataInitializer.shutdownMaterializerAndActorSystem()

  implicit class StringExtensions(s: String) {
    def takeRightWhile(p: (Char) => Boolean): String = s.takeRight(s.reverseIterator.takeWhile(p).length)
  }

  def createCassandraRackProperties(hosts: ParSeq[String] = ips.par) {
    hosts.zipWithIndex.foreach { ip: (String, Int) =>
      val content = s"dc=DC1\nrack=RAC${ip._2 + 1}"
      command(s"""echo "$content" > ${instDirs.globalLocation}/cm-well/conf/cas/cassandra-rackdc.properties""",
        ip._1,
        false)
      for (i <- 2 to dataDirs.casDataDirs.size)
        command(s"""echo "$content" > ${instDirs.globalLocation}/cm-well/conf/cas$i/cassandra-rackdc.properties""",
          ip._1,
          false)
    }
  }

  def createUser(user: String = "u", pass: String = "said2000", hosts: ParSeq[String] = ips.par, sudoer: Credentials) {
    command(s"sudo useradd $user", hosts, true, Some(sudoer))
    command(s"echo '$user:$$USERPASS' | sudo chpasswd", hosts, true, Some(sudoer), Map("USERPASS" -> pass))
  }

  def sudoComm(com: String) = s"""sudo bash -c \"\"\"${com}\"\"\""""

  def elasticsearchStatus(host: String) = {
    val r = command("curl -sX GET http://" + ips(0) + esHealthAddress, host, false)
    r match {
      case Success(v) =>
        Try(JSON.parseFull(v.trim).get.asInstanceOf[Map[String, Any]]("status").toString.trim.split('.')(0))
      case Failure(e) => Failure(e)
    }
  }

  def command(com: String, hosts: ParSeq[String], sudo: Boolean): ParSeq[Try[String]] = {
    command(com, hosts, sudo, None)
  }

  def command(com: String, hosts: ParSeq[String], sudo: Boolean, sudoer: Option[Credentials]): ParSeq[Try[String]] = {
    hosts.map { host =>
      command(com, host, sudo, sudoer)
    }
  }

  def command(com: String,
              hosts: ParSeq[String],
              sudo: Boolean,
              sudoer: Option[Credentials],
              variables: Map[String, String]): ParSeq[Try[String]] = {
    hosts.map(host => command(com, host, sudo, sudoer, variables))
  }

  def command(com: String, host: String, sudo: Boolean): Try[String] = {
    command(com, host, sudo, None)
  }

  def command(com: String, host: String, sudo: Boolean, sudoer: Option[Credentials]): Try[String] = {
    command(com, host, sudo, sudoer, Map[String, String]())
  }

  def command(com: String,
              host: String,
              sudo: Boolean,
              sudoer: Option[Credentials],
              variables: Map[String, String]): Try[String] = {
    if (sudo && isSu && sudoer.isEmpty)
      throw new Exception(s"Sudoer credentials must be available in order to use sudo")
    if (ips.indexOf(host) == -1 && host != haProxy.map(x => x.host).getOrElse(""))
      throw new Exception(s"The host $host is not part of this cluster")
    val (readVarsLine, varValues) = variables.fold(("", "")) {
      case ((readVarsStr, varValuesForEcho), (varName, value)) =>
        (s"$readVarsStr read $varName;", s"$varValuesForEcho$value\\\\n")
    }
    val (commandLine, process) = if (sudo && isSu) {

      // scalastyle:off
      //old version that get stuck sometimes - val command = s"""ssh -o StrictHostKeyChecking=no ${sudoer.get.name}@$host bash -c $$'{ export PATH=$path; read PASS; ./sshpass -p $$PASS bash -c "${escapedCommand(com)}"; }'"""
      val cmd = s"""ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR ${sudoer.get.name}@$host export PATH=$path;$readVarsLine read PASS; sshpass -p $$PASS 'bash -c "${escapedCommand(com)}"'"""
      // scalastyle:on
      (cmd, Seq("bash", "-c", s"echo -e $varValues${sudoer.get.pass}") #| cmd)
    } else {
      if (variables.nonEmpty) {
        val cmd =
          s"""ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR $user@$host export PATH=$path;$readVarsLine bash -c "${escapedCommand(
            com
          )}""""
        (cmd, Seq("bash", "-c", s"echo -e ${varValues.dropRight(1)}") #| cmd)
      } else {
        val cmd =
          Seq("ssh", "-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR", s"$user@$host", s"PATH=$path $com")
        (cmd.mkString(" "), Process(cmd))
      }
    }
    // scalastyle:off
    if (verbose) println("command: " + commandLine)
    // scalastyle:on
    Try(process.!!)
  }

  private def escapedCommand(cmd: String) =
  // old version for $'..' bash string: cmd.replace("\"", "\\\\\"").replace("'", "\\'")
  // for scala 2.12 it was required: .replace("\"", "\\\"").replace("'", "\\\\'")
    cmd

  def command(com: String, sudo: Boolean = false): Try[String] = {
    if (sudo && isSu)
      Try(sudoComm(com).!!)
    else {
      val seq = Seq("bash", "-c", com)
      // scalastyle:off
      if (verbose) println("local command: " + seq.mkString(" "))
      // scalastyle:on
      Try(seq.!!)
    }
  }

  def rsync(from: String, to: String, hosts: ParSeq[String], sudo: Boolean = false): ParSeq[Try[String]] = {
    val h = hosts.map(host => if (host.indexOf("@") == -1) s"${user}@${host}" else host)
    h.map { host =>
      _rsync(from, to, host, sudo = sudo)
    }
  }

  def _rsync(from: String, to: String, host: String, tries: Int = 10, sudo: Boolean): Try[String] = {
    val seq = Seq("rsync", "-e", "ssh -o LogLevel=ERROR", "-Paz", "--delete", from, host + ":" + to)

    // scalastyle:off
    if (verbose) println("command: " + seq.mkString(" "))
    // scalastyle:on
    val res = Try(seq.!!)
    res match {
      case Success(r)   => res
      case Failure(err) => if (tries == 0) res else _rsync(from, to, host, tries - 1, sudo)
    }
  }

  def removeDataDirs: Unit = removeDataDirs()

  def removeDataDirs(i: ParSeq[String] = ips.par) {

    command(s"rm -rf ${instDirs.intallationDir}", i, false)

    dataDirs.casDataDirs.foreach { cas =>
      command(s"rm -rf ${cas}", i, false)
    }

    dataDirs.casCommitLogDirs.foreach { ccl =>
      command(s"rm -rf ${ccl}", i, false)
    }

    dataDirs.esDataDirs.foreach { es =>
      command(s"rm -rf ${es}", i, false)
    }

    command(s"rm -rf ${dataDirs.logsDataDir}", i, false)
  }

  def createDataDirs(): Unit = createDataDirs(ips.par)

  def createDataDirs(hosts: ParSeq[String]) {
    info("creating data directories")

    info("  creating installation directory")
    command(s"mkdir -p ${instDirs.intallationDir}/", hosts, false)
    deployment.componentProps.collect { case cp: DataComponent => cp }.foreach {
      _.createDataDirectories(hosts)
    }

    info("  creating log data directory")
    command(s"mkdir -p ${dataDirs.logsDataDir}", hosts, false)

    info("finished creating data directories")
  }

  def deployComponents(hosts: ParSeq[String] = ips.par) {
    deployment.componentProps.foreach(_.deployComponent(hosts = hosts))
  }

  def genResources(hosts: ParSeq[String] = ips.to(ParSeq)) {
    deployment.createResources(mkScripts(hosts))
  }

  def genEsResources(hosts: ParSeq[String]) {
    deployment.createResources(mkScripts(hosts).filter(_.isInstanceOf[ElasticsearchConf]))
  }

  def genCtrlResources(hosts: ParSeq[String]) {
    deployment.createResources(mkScripts(hosts).filter(_.isInstanceOf[CtrlConf]))
  }

  def deployApplication: Unit = deployApplication()

  def deployApplication(hosts: ParSeq[String] = ips.par) {
    syncLib(hosts)
    info("deploying application")
    info("  creating application directories")
    //command(s"mkdir -p ${instDirs.intallationDir}/", hosts, false)

    // scalastyle:off
    command(s"mkdir ${instDirs.intallationDir}/app ${instDirs.intallationDir}/conf ${instDirs.intallationDir}/data ${instDirs.intallationDir}/bin", hosts, false)
    command(s"mkdir ${instDirs.intallationDir}/app/bg ${instDirs.intallationDir}/app/ctrl ${instDirs.intallationDir}/app/dc ${instDirs.intallationDir}/app/cas ${instDirs.intallationDir}/app/es ${instDirs.intallationDir}/app/ws ${instDirs.intallationDir}/app/scripts ${instDirs.intallationDir}/app/tools", hosts, false)
    // scalastyle:on
    command(s"ln -s ${dataDirs.logsDataDir} ${instDirs.intallationDir}/log", hosts, false)
    info("  deploying components")
    deployComponents(hosts)
    //info("  extracting components")
    //extractComponents(hosts)
    info("  creating symbolic links")

    deployment.componentProps.collect { case cp: DataComponent => cp }.foreach {
      _.linkDataDirectories(hosts)
    }
    deployment.componentProps.collect { case cp: LoggingComponent => cp }.foreach {
      _.createLoggingDirectories(hosts)
    }
    deployment.componentProps.collect { case cp: ConfigurableComponent => cp }.foreach {
      _.createConigurationsDirectoires(hosts)
    }

    rsync("./scripts/", s"${instDirs.intallationDir}/app/scripts/", hosts)

    info("  creating links in app directory")
    createAppLinks(hosts)
    rsync(s"./components/mx4j-tools-3.0.1.jar", s"${instDirs.intallationDir}/app/cas/cur/lib/", hosts)
    info("  creating scripts")
    genResources(hosts)
    info("  deploying plugins")
    rsyncPlugins(hosts)
    info("  linking libs")
    linkLibs(hosts)
    info("finished deploying application")
  }

   def verifyConfigsNotChanged = {
     info("verify that configuration files have not been changed")
     //It is taken from cassandra version 3.11.5
     UtilCommands.verifyComponentConfNotChanged("apache-cassandra", "conf/cassandra.yaml", "9682ae0951f560a8480c1ebb656caae7")
     UtilCommands.verifyComponentConfNotChanged("apache-cassandra", "conf/cassandra-env.sh", "f0b72c8f2301d815acea81bd6633aa3a")
     UtilCommands.verifyComponentConfNotChanged("apache-cassandra", "conf/jvm.options", "3fc118b8d5d3b24331b205e2a2d24cb0")
     UtilCommands.verifyComponentConfNotChanged("apache-cassandra", "conf/logback.xml", "cc17f60a18c6b7b5a797d2edd2c91119")
     UtilCommands.verifyComponentConfNotChanged("apache-cassandra", "bin/cassandra", "5d8bba323cb91c3deb8c140cdbd34d5d")
     UtilCommands.verifyComponentConfNotChanged("apache-cassandra", "bin/cassandra.in.sh", "851a2a0514826162682a25953e710493")
     //elasticsearch checksums were taken from version 7.4.2
     UtilCommands.verifyComponentConfNotChanged("elasticsearch", "config/elasticsearch.yml", "4f96a88585ab67663ccbca1c43649ed5")
     UtilCommands.verifyComponentConfNotChanged("elasticsearch", "config/jvm.options", "a80a0a9b5e95d5cdc5f4c3088f0d801e")
     UtilCommands.verifyComponentConfNotChanged("elasticsearch", "config/log4j2.properties", "dbb23d025177409bdb734f3ad3efd147")
   }

  private def createAppLinks(hosts: ParSeq[String]) = {
    // scalastyle:off
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/bg/logs || ln -s ${instDirs.globalLocation}/cm-well/log/bg/ ${instDirs.globalLocation}/cm-well/app/bg/logs", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ws/logs || ln -s ${instDirs.globalLocation}/cm-well/log/ws/ ${instDirs.globalLocation}/cm-well/app/ws/logs", hosts, false)
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/log/cw/", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ws/cw-logs || ln -s ${instDirs.globalLocation}/cm-well/log/cw/ ${instDirs.globalLocation}/cm-well/app/ws/cw-logs", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ctrl/logs || ln -s ${instDirs.globalLocation}/cm-well/log/ctrl/ ${instDirs.globalLocation}/cm-well/app/ctrl/logs", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/dc/logs || ln -s ${instDirs.globalLocation}/cm-well/log/dc/ ${instDirs.globalLocation}/cm-well/app/dc/logs", hosts, false)

    command(s"test -L ${instDirs.globalLocation}/cm-well/app/bg/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/bg/ ${instDirs.globalLocation}/cm-well/app/bg/conf", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ws/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/ws/ ${instDirs.globalLocation}/cm-well/app/ws/conf", hosts, false)
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/conf/cw/", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ws/cw-conf || ln -s ${instDirs.globalLocation}/cm-well/conf/cw/ ${instDirs.globalLocation}/cm-well/app/ws/cw-conf", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ctrl/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/ctrl/ ${instDirs.globalLocation}/cm-well/app/ctrl/conf", hosts, false)
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/conf/dc/", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/dc/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/dc/ ${instDirs.globalLocation}/cm-well/app/dc/conf", hosts, false)
    // scalastyle:on
  }

  def mkScripts(ips: ParSeq[String] = ips.to(ParSeq)): ParSeq[ComponentConf] = {
    null
  }

  def redeploy: Unit = redeploy()

  def redeploy(hosts: ParSeq[String] = ips.par) {
    checkProduction
    stop(false, hosts)
    clearApp(hosts)
    deployApplication(hosts)
  }

  def updateWebService: Unit = updateWebService()

  def updateWebService(hosts: ParSeq[String] = ips.to(ParSeq)) {
    hosts.foreach { h =>
      command(s"cd ${instDirs.globalLocation}/cm-well/app/ws; mkdir tmp", ParSeq(h), false)
      rsync("./components/cmwell-ws_2.10-1.0.1-SNAPSHOT-dist.zip",
        s"${instDirs.globalLocation}/cm-well/app/ws/tmp/cmwell-ws_2.10-1.0.1-SNAPSHOT-dist.zip",
        ParSeq(h))
      command(s"cd ${instDirs.globalLocation}/cm-well/app/ws/tmp; unzip cmwell-ws_2.10-1.0.1-SNAPSHOT-dist.zip",
        hosts,
        false)
      stopWebservice(ParSeq(h))
      command(s"rm -rf ${instDirs.intallationDir}/cm-well/app/ws/cmwell-ws-1.0.1-SNAPSHOT", ParSeq(h), false)
      command(s"rm ${instDirs.globalLocation}/cm-well/app/ws/RUNNING_PID", ParSeq(h), false)
      command(
        s"mv ${instDirs.globalLocation}/cm-well/app/ws/tmp/cmwell-ws-1.0.1-SNAPSHOT ${instDirs.globalLocation}/cm-well/app/ws/cmwell-ws-1.0.1-SNAPSHOT",
        ParSeq(h),
        false
      )
      startWebservice(ParSeq(h))
    }
  }

  def removeCmwellSymLink(): Unit = removeCmwellSymLink(ips.par)

  def removeCmwellSymLink(hosts: ParSeq[String]) {
    command(s"unlink ${instDirs.globalLocation}/cm-well 2> /dev/null", hosts, false)
  }

  def createCmwellSymLink(sudoer: Option[Credentials]): Unit = createCmwellSymLink(ips.par, sudoer)

  def createCmwellSymLink(hosts: ParSeq[String], sudoer: Option[Credentials] = None) {
    removeCmwellSymLink(hosts)
    command(
      s"sudo ln -s ${instDirs.intallationDir} ${instDirs.globalLocation}/cm-well; sudo chown -h $user:$user ${instDirs.globalLocation}/cm-well",
      hosts,
      true,
      sudoer
    )
  }

  def registerCtrlService(hosts: ParSeq[String], sudoer: Credentials) {
    if (ctrlService) {
      //remove the old ctrl (the link one) - if exists
      command("sudo rm -f /etc/init.d/ctrl", hosts, true, Some(sudoer))
      command(s"mkdir -p ${instDirs.globalLocation}/cm-well/conf/ctrl", hosts, false)
      createFile(s"${instDirs.globalLocation}/cm-well/conf/ctrl/ctrl",
        Source.fromFile("scripts/templates/ctrl").mkString.replace("{{user}}", user),
        hosts)
      command(s"chmod +x ${instDirs.globalLocation}/cm-well/conf/ctrl/ctrl", hosts, false)
      val cmwellRunner = Source.fromFile("scripts/templates/cmwell-runner").mkString
      createFile("/etc/init.d/cmwell-runner", cmwellRunner, hosts, true, Some(sudoer))
      command("sudo chmod +x /etc/init.d/cmwell-runner", hosts, true, Some(sudoer))
      hosts.foreach { host =>
        getOs(host) match {
          case Ubuntu =>
            command("sudo update-rc.d cmwell-runner defaults", host, true, Some(sudoer))
          case Oracle =>
            command("sudo chkconfig --add cmwell-runner", host, true, Some(sudoer))
            command("sudo chkconfig cmwell-runner on", host, true, Some(sudoer))
        }
      }
      command("sudo service cmwell-runner start", hosts, true, Some(sudoer))
    }
  }

  def disks: Set[String] = {
    val DataDirs(casDataDirs, casCommitLogDirs, esDataDirs, kafkaDataDirs, zookeeperDataDir, logsDataDir) = dataDirs
    val dirs = casDataDirs ++ casCommitLogDirs ++ esDataDirs ++ kafkaDataDirs ++ Seq(zookeeperDataDir,
      logsDataDir,
      instDirs.intallationDir)
    dirs.map(dir => dir.substring(0, dir.lastIndexOf("/"))).toSet
  }

  def disksWithAncestors(disks: Set[String]): Set[String] = {
    def addSlash(p: String) = p match {
      case ""  => "";
      case "/" => "/";
      case _   => p + "/"
    }

    disks.flatten { disk =>
      val splitted = disk.split("/").map(p => if (p.isEmpty) "/" else p)
      val ancestors = splitted.scan("")((p, dir) => addSlash(p) + dir)
      ancestors.filterNot(p => p.isEmpty || p == "/")
    }
  }

  def unprepareMachines(): Unit = unprepareMachines(ips.par)

  def unprepareMachines(hosts: ParSeq[String]) {
    purge(hosts)
    removeDataDirs(hosts)
    removeCmwellSymLink(hosts)
  }

  def changeOwnerAndAddExcutePermission(hosts: ParSeq[String],
                                        dirs: Seq[String],
                                        user: String,
                                        sudoer: Credentials): Unit = {
    dirs.par.foreach(dir => command(s"sudo chmod +x $dir; sudo chown $user:$user $dir", hosts, true, Some(sudoer)))
  }

  def changeKernelSettings(user: String, sudoer: Credentials, hosts: ParSeq[String]): Unit = {
    hosts.foreach { host =>
      val cassandraKernelConf = Source.fromFile("scripts/templates/60-cassandra.conf").mkString
      createFile("/etc/security/limits.d/60-cassandra.conf", cassandraKernelConf, host, true, Some(sudoer))
      val maxMapCount = command(s"sudo sysctl -n vm.max_map_count", host, true, Some(sudoer)).map(_.trim.toLong)
      maxMapCount match {
        case Success(currentMax) =>
          //Make sure to change 60-cm-well.conf accordingly
          val minimumRequiredByEs = 262144
          val minimumRequiredByCas = 1048575
          val minimumRequired = Math.max(minimumRequiredByCas, minimumRequiredByEs)
          if (currentMax < minimumRequired) {
            val cmwellKernelConf = Source.fromFile("scripts/templates/60-cm-well.conf").mkString
            createFile("/etc/sysctl.d/60-cm-well.conf", cmwellKernelConf, host, true, Some(sudoer))
            command("sudo sysctl -p /etc/sysctl.d/60-cm-well.conf", host, true, Some(sudoer))
          }
        // scalastyle:off
        case Failure(ex) => println(s"Failed getting vm.max_map_count from host $host. The execption message is: ${ex.getMessage}")
        // scalastyle:on
      }
    }
  }



  def prepareMachines(): Unit = prepareMachines(ips.par, "", "", "")

  def prepareMachines(hosts: String*): Unit = prepareMachines(hosts.to(ParSeq), "", "", "")

  def prepareMachines(hosts: ParSeq[String], sudoerName: String, sudoerPass: String, userPass: String) {
    val sudoerNameFinal: String =
      if (sudoerName != "") sudoerName else scala.io.StdIn.readLine("Please enter sudoer username\n")
    val sudoerPassword: String =
      if (sudoerPass != "") sudoerPass else scala.io.StdIn.readLine(s"Please enter $sudoerNameFinal password\n")

    // scalastyle:off
    println(s"Gaining trust of sudoer account: $sudoerNameFinal")
    // scalastyle:on
    gainTrust(sudoerNameFinal, sudoerPassword, hosts)
    sudoerCredentials = Some(Credentials(sudoerNameFinal, sudoerPassword))
    val sudoer = sudoerCredentials.get
    copySshpass(hosts, sudoer)
    // scalastyle:off
    println("We will now create a local user 'u' for this cluster")
    // scalastyle:on
    val pass = if (userPass != "") userPass else scala.io.StdIn.readLine(s"Please enter $user password\n")
    createUser(user, pass, hosts, sudoer)
    // scalastyle:off
    println(s"Gaining trust of the account $user")
    // scalastyle:on
    gainTrust(user, pass, hosts)
    refreshUserState(user, Some(sudoer), hosts)
    changeOwnerAndAddExcutePermission(hosts, disksWithAncestors(disks).toSeq, user, sudoer)
    changeKernelSettings(user, sudoer, hosts)
    createDataDirs(hosts)
    createCmwellSymLink(hosts, Some(sudoer))
    registerCtrlService(hosts, sudoer)
    finishPrepareMachines(hosts, sudoer)
  }

  protected def finishPrepareMachines(hosts: ParSeq[String], sudoer: Credentials) = {
     deleteSshpass(hosts, sudoer)
    info("Machine preparation was done. Please look at the console output to see if there were any errors.")
  }

  private def copySshpass(hosts: ParSeq[String], sudoer: Credentials): Unit = {
    //only copy sshpass if it's an internal one
    if (UtilCommands.linuxSshpass == "bin/utils/sshpass") {
      hosts.foreach(host => s"""ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR ${sudoer.name}@$host mkdir -p '~/bin'""".!!)
      hosts.foreach(
        host =>
          Seq("rsync",
            "-z",
            "-e",
            "ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR",
            UtilCommands.linuxSshpass,
            s"${sudoer.name}@$host:~/bin") !!
      )
    }
  }

  private def deleteSshpass(hosts: ParSeq[String], sudoer: Credentials): Unit = {
    command("rm ~/bin/sshpass", hosts, true, Some(sudoer))

  }

  def prepareMachinesNonInteractive: Unit = prepareMachinesNonInteractive()

  def prepareMachinesNonInteractive(sudoerName: String = "mySudoer",
                                    sudoerPass: String = "said2000",
                                    uPass: String = "said2000",
                                    hosts: ParSeq[String] = ips.par) {
    gainTrust(sudoerName, sudoerPass, hosts)
    val sudoer = Credentials(sudoerName, sudoerPass)
    sudoerCredentials = Some(sudoer)
    copySshpass(hosts, sudoer)
    createUser(pass = uPass, hosts = hosts, sudoer = sudoer)
    gainTrust("u", uPass, hosts)
    refreshUserState("u", Some(sudoer))
    changeOwnerAndAddExcutePermission(hosts, disksWithAncestors(disks).toSeq, user, sudoer)
    createDataDirs()
    createCmwellSymLink(Some(sudoer))
    registerCtrlService(hosts, sudoer)
    //    deleteSshpass(hosts, sudoer)
  }

  def deploy: Unit = deploy()

  def deploy(hosts: ParSeq[String] = ips.par) {
    checkProduction
    deployApplication(hosts)
  }

  def getNewHostInstance(ipms: Seq[String]): Host

  def cassandraNetstats = {
    // scalastyle:off
    println(command(s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath netstats 2> /dev/null", ips(0), false).get)
    // scalastyle:on
  }

  def removeNode(host: String): Unit = {
    checkProduction
    connectToGrid
    if (CtrlClient.currentHost == host) CtrlClient.init((ips.toSet - host).head)

    purge(ParSeq(host))
    Host.ctrl.waitForHealth
    Thread.sleep(20000)
    info("Removing node from Grid")
    Host.ctrl.removeNode(host)

  }

  def addNodesSH(hosts: Seq[String]) {
    addNodes(hosts)
    sys.exit(0)
  }

  def removeNodeSH(ip: String) {
    removeNode(ip)
    sys.exit(0)
  }

  def addNodes(ipms: Seq[String], sudoerName: String = "", sudoerPass: String = "", userPass: String = ""): Host = {
    connectToGrid
    val addedInstances = getNewHostInstance(ipms)

    //Due to Dudi's request prepare machine isn't run by default and must be run manually (to spare the need for passwords)
    //addedInstances.prepareMachines(addedInstances.ips.par, sudoerName = sudoerName, sudoerPass = sudoerPass, userPass = userPass)
    addedInstances.purge()

    this.deploy(addedInstances.ips.to(ParSeq))
    this.startCtrl(addedInstances.ips.to(ParSeq))

    Thread.sleep(20000)

    ipms.foreach(Host.ctrl.addNode)

    this.startDcForced(addedInstances.ips.to(ParSeq))

    addedInstances  }

  def killProcess(name: String, flag: String, hosts: ParSeq[String] = ips.par, tries: Int = 5) {
    if (tries > 0) {
      command(s"ps aux | grep -v grep | grep $name | awk '{print $$2}' | xargs -I zzz kill $flag zzz 2> /dev/null",
        hosts,
        false)
      val died = command(s"ps aux | grep java | grep -v grep | grep $name | wc -l ", hosts, false)
        .map(s => s.get.trim.toInt)
        .filterNot(_ == 0)
        .length == 0
      if (!died) {
        Thread.sleep(500)
        killProcess(name, flag, hosts, tries - 1)
      }
    } else {
      command(s"ps aux | grep java | grep " + name + " | awk '{print $2}' | xargs -I zzz kill -9 zzz 2> /dev/null",
        hosts,
        false)
    }
  }

  // todo: kill with -9 if it didn't work.
  // todo: remove es with its command.
  def stop: Unit = stop(false, ips.par)

  def stop(hosts: String*): Unit = stop(false, hosts.par)

  def stop(force: Boolean, hosts: ParSeq[String]) {
    checkProduction
    val tries = if (force) 0 else 5
    stopWebservice(hosts, tries)
    stopBg(hosts, tries)
    stopElasticsearch(hosts, tries)
    stopCassandra(hosts, tries)
    stopCtrl(hosts, tries)
    stopCW(hosts, tries)
    stopDc(hosts, tries)
    stopKafka(hosts, tries)
    stopZookeeper(hosts, tries)
  }

  def clearData: Unit = clearData()

  def clearData(hosts: ParSeq[String] = ips.par) {
    checkProduction
    dataDirs.casDataDirs.foreach { cas =>
      command(s"rm -rf ${cas}/*", hosts, false)
    }

    dataDirs.casCommitLogDirs.foreach { ccl =>
      command(s"rm -rf ${ccl}/*", hosts, false)
    }

    dataDirs.esDataDirs.foreach { es =>
      command(s"rm -rf ${es}/*", hosts, false)
    }

    dataDirs.kafkaDataDirs.foreach { kafka =>
      command(s"rm -rf $kafka/*", hosts, false)
    }

    command(s"rm -rf ${dataDirs.zookeeperDataDir}/*", hosts, false)

    command(s"rm -rf ${dataDirs.logsDataDir}/*", hosts, false)
  }

  def clearApp: Unit = clearApp()

  def clearApp(hosts: ParSeq[String] = ips.par) {
    checkProduction
    command(s"rm -rf ${instDirs.intallationDir}/*", hosts, false)
  }

  def purge: Unit = purge()

  def purge(hosts: ParSeq[String] = ips.to(ParSeq)) {
    checkProduction
    info("purging cm-well")
    info("  stopping processes")
    stop(true, hosts)
    info("  clearing application data")
    clearApp(hosts)
    info("  clearing data")
    clearData(hosts)
    info("finished purging cm-well")
  }

  def injectMetaData: Unit = injectMetaData(ips(0))

  def injectMetaData(host: String) {
    dataInitializer.uploadMetaData()
    dataInitializer.uploadNameSpaces()
  }

  def injectSampleData = {
    dataInitializer.uploadSampleData()
  }

  def casHealth: Try[String] = casHealth()

  def casHealth(hosts: ParSeq[String] = ips.par): Try[String] = {
    command(s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin" + nodeToolPath + " status", hosts(0), false)
  }

  def esHealth: Try[String] = {
    command("curl -sX GET http://" + pingAddress + esHealthAddress, ips(0), false)
  }

  def stopBg: Unit = stopBg(ips.par)

  def stopBg(hosts: String*): Unit = stopBg(hosts.par)

  def stopBg(hosts: ParSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("cmwell.bg.Runner", "", hosts, tries)
  }

  def stopWebservice: Unit = stopWebservice(ips.par)

  def stopWebservice(hosts: String*): Unit = stopWebservice(hosts.par)

  def stopWebservice(hosts: ParSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("psId=Webserver", "", hosts, tries)
  }

  def stopCW: Unit = stopCW(ips.par)

  def stopCW(hosts: String*): Unit = stopCW(hosts.par)

  def stopCW(hosts: ParSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("crashableworker", "", hosts, tries)
  }

  def stopDc: Unit = stopDc(ips.par)

  def stopDc(hosts: String*): Unit = stopDc(hosts.par)

  def stopDc(hosts: ParSeq[String], tries: Int = 5) = {
    checkProduction
    killProcess("app/dc", "", hosts, tries)
  }

  def stopCassandra: Unit = stopCassandra(ips.par)

  def stopCassandra(hosts: String*): Unit = stopCassandra(hosts.par)

  def stopCassandra(hosts: ParSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("CassandraDaemon", "", hosts, tries)
  }

  def esSyncedFlush(host: String, port: Int = 9200): Unit = {
    command(s"curl -sX POST 'http://$host:$port/_all/_flush/synced'")
  }

  def stopElasticsearch: Unit = {
    stopElasticsearch(ips.par)
  }

  def stopElasticsearch(hosts: String*): Unit = stopElasticsearch(hosts.par)

  def stopElasticsearch(hosts: ParSeq[String], tries: Int = 5) {
    checkProduction
    esSyncedFlush(hosts(0))
    killProcess("Elasticsearch", "", hosts, tries)
  }

  def startBg: Unit = startBg(ips.par)

  def startBg(hosts: String*): Unit = startBg(hosts.par)

  def startBg(hosts: ParSeq[String]) {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/bg; ${startScript("./start.sh")}", hosts, false)
  }

  def startWebservice: Unit = startWebservice(ips.par)

  def startWebservice(hosts: String*): Unit = startWebservice(hosts.par)

  def startWebservice(hosts: ParSeq[String]) {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/ws/; ${startScript("./start.sh")}", hosts, false)
  }

  def startCW: Unit = startCW(ips.par)

  def startCW(hosts: String*): Unit = startCW(hosts.par)

  def startCW(hosts: ParSeq[String]) {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/ws; ${startScript("./cw-start.sh")}", hosts, false)
  }

  def startDc: Unit = startDc(ips.par)

  def startDc(hosts: String*): Unit = startDc(hosts.par)

  def startDc(hosts: ParSeq[String]): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/dc; ${startScript("./start.sh")}", hosts, false)
  }

  def startDcForced(hosts: ParSeq[String]): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/dc; HAL=9000 FORCE=MAJOUR ./start.sh", hosts, false)
  }

  //def startScript(script : String) = s"""bash -c "starter '$script'" > /dev/null 2> /dev/null & """
  //def startScript(script : String) = s"""HAL=9000 $script"""
  def startScript(script: String) =
    s"""HAL=9000 ${if (deb) "CMWELL_DEBUG=true" else ""} $script"""

  def start: Unit = start(ips)

  def start(hosts: Seq[String]) {
    val parHosts = hosts.to(ParSeq)
    checkProduction
    startCassandra(parHosts)
    startElasticsearch(hosts)

    Try(CassandraLock().waitForModule(hosts(0), casSize))
    Try(ElasticsearchLock().waitForModule(hosts(0), esSize))
    startZookeeper
    startKafka(parHosts)

    startCtrl(parHosts)
    startBg(parHosts)
    startCW(parHosts)
    startWebservice(parHosts)
    startDc(parHosts)
  }

  def startCtrl: Unit = startCtrl(ips.to(ParSeq))

  def startCtrl(hosts: String*): Unit = startCtrl(hosts.par)

  def startCtrl(hosts: ParSeq[String]) = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/ctrl; ${startScript("./start.sh")}", hosts, false)
  }

  def stopCtrl: Unit = stopCtrl(ips.par)

  def stopCtrl(hosts: String*): Unit = stopCtrl(hosts.par)

  def stopCtrl(hosts: ParSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("CtrlServer", "", hosts, tries)
  }

  def createManager: Unit = createManager()

  def createManager(machineName: String = ips(0), path: String = "~/cmwell/") {
    rsync("./", path, ParSeq(machineName))
  }

  def readTime(targ: String = "meta/ns/oa") {}

  def init: Unit = init()

  def init(hosts: Seq[String] = ips) {
    val parHosts = hosts.to(ParSeq)
    checkProduction
    info("starting controller")
    startCtrl(parHosts)
    info("initializing cm-well")
    info("  initializing cassandra")
    initCassandra(parHosts)
    info("  initializing elasticsearch")
    initElasticsearch(hosts)
    info("  waiting for cassandra and elasticsearch")

    Retry {
      try {
        CassandraLock().waitForModule(hosts(0), casSize)
      } catch {
        case t: Throwable =>
          info("Trying to reinit Cassandra")
          initCassandra(parHosts)
          throw t
      }
    }

    Retry {
      try {
        ElasticsearchLock().waitForModule(hosts(0), esSize)
      } catch {
        case t: Throwable =>
          info("Trying to reinit Elasticsearch")
          initElasticsearch(hosts)
          throw t
      }
    }

    info("  starting zookeeper")
    startZookeeper
    info("  starting kafka")
    startKafka

    info("  inserting schemas")
    initSchemes(parHosts)
    // wait until all the schemas are written.
    Thread.sleep(10000)

    info("  starting bg")
    startBg(parHosts)
    info(" starting cw")
    startCW(parHosts)
    info("  starting web service")
    startWebservice(parHosts)
    uploadInitialContent(hosts(0))
    info("  starting dc controller")
    startDc(parHosts)

    info("finished initializing cm-well")
  }

  def uploadInitialContent(host: String = ips(0)): Unit = {
    checkProduction
    Try(WebServiceLock().waitForModule(host, 1))

    info("  waiting for ws...")
    dataInitializer.waitForWs()
    info("  inserting meta data")
    injectMetaData(host)
    info("  uploading SPAs to meta/app")
    dataInitializer.uploadDirectory("data", s"http://$host:9000/meta/app/")
    info("  uploading docs")
    dataInitializer.uploadDirectory("docs", s"http://$host:9000/meta/docs/")
    info("  uploading basic userInfotons (if not exist)")
    dataInitializer.uploadBasicUserInfotons(host)
    info("  updating version history")
    dataInitializer.logVersionUpgrade(host)
  }

  def initCassandra: Unit = initCassandra()

  def initCassandra(hosts: ParSeq[String] = ips.par)

  def initElasticsearch: Unit = initElasticsearch()

  def initElasticsearch(hosts: Seq[String] = ips)

  def initSchemes: Unit = initSchemes()

  def initSchemes(hosts: ParSeq[String] = ips.par) {
    // scalastyle:off
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur; sh bin/cqlsh ${pingAddress} -f ${instDirs.globalLocation}/cm-well/conf/cas/cassandra-cql-init-cluster-new", hosts(0), false)
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur; sh bin/cqlsh ${pingAddress} -f ${instDirs.globalLocation}/cm-well/conf/cas/zstore-cql-init-cluster", hosts(0), false)
    val templateCreation = command(s"""unset http_proxy;curl -s -X POST http://${hosts(0)}:$esMasterPort/_template/cmwell_index_template -H "Content-Type: application/json" --data-ascii @${instDirs.globalLocation}/cm-well/conf/es/indices_template_new.json""", hosts(0), false)
    templateCreation match {
      case Success(res) =>
        if (res.trim != """{"acknowledged":true}""") {
          println(s"Elasticsearch template creation failed. The response was: $res")
          sys.exit(1)
        }
      case Failure(ex) =>
        println(s"Elasticsearch template creation failed with: $ex")
        sys.exit(1)
    }
    //create the first index in advance. It resolves the issue of meta ns cache quering a non existant index
    val firstIndexCreation = command(s"""unset http_proxy;curl -s -X PUT http://${hosts(0)}:$esMasterPort/cm_well_p0_0""", hosts(0), false)
    firstIndexCreation match {
      case Success(res) =>
        if (res.trim != """{"acknowledged":true,"shards_acknowledged":true,"index":"cm_well_p0_0"}""") {
          println(s"Elasticsearch first index creation failed. The response was: $res")
          sys.exit(1)
        }
      case Failure(ex) =>
        println(s"Elasticsearch first index creation failed with: $ex")
        sys.exit(1)
    }
    // create kafka topics
    val replicationFactor = math.min(hosts.size, 3)

    val javaHomeLocation = s"${instDirs.globalLocation}/cm-well/app/java"
    val exportCommand = s"""if [ -d $javaHomeLocation ] ;
        then export PATH=$javaHomeLocation/bin:$$PATH ;
        export JAVA_HOME=$javaHomeLocation ;
        fi ; """

    // scalastyle:off
    val createTopicCommandPrefix = s"cd ${instDirs.globalLocation}/cm-well/app/kafka/cur; $exportCommand sh bin/kafka-topics.sh --create --zookeeper ${pingAddress}:2181 --replication-factor $replicationFactor --partitions ${hosts.size} --topic"

    // scalastyle:on
    var tryNum: Int = 1
    var ret = command(s"$createTopicCommandPrefix persist_topic", hosts(0), false)
    while (ret.isFailure || !ret.get.contains("Created topic") && tryNum < 6) {
      tryNum += 1
      Thread.sleep(5000)
      ret = command(s"$createTopicCommandPrefix persist_topic", hosts(0), false)
    }

    ret = command(s"$createTopicCommandPrefix persist_topic.priority", hosts(0), false)
    while (ret.isFailure || !ret.get.contains("Created topic") && tryNum < 6) {
      tryNum += 1
      Thread.sleep(5000)
      ret = command(s"$createTopicCommandPrefix persist_topic.priority", hosts(0), false)
    }

    ret = command(s"$createTopicCommandPrefix index_topic", hosts(0), false)
    while (ret.isFailure || !ret.get.contains("Created topic") && tryNum < 6) {
      tryNum += 1
      Thread.sleep(5000)
      ret = command(s"$createTopicCommandPrefix index_topic", hosts(0), false)
    }

    ret = command(s"$createTopicCommandPrefix index_topic.priority", hosts(0), false)
    while (ret.isFailure || !ret.get.contains("Created topic") && tryNum < 6) {
      tryNum += 1
      Thread.sleep(5000)
      ret = command(s"$createTopicCommandPrefix index_topic.priority", hosts(0), false)
    }

    ret = command(s"$createTopicCommandPrefix red_queue", hosts(0), false)
    while (ret.isFailure || !ret.get.contains("Created topic") && tryNum < 6) {
      tryNum += 1
      Thread.sleep(5000)
      ret = command(s"$createTopicCommandPrefix red_queue", hosts(0), false)
    }
  }

  def avaiableHosts = {
    ips.filter { ip =>
      command(s"ping -c 1 $ip > /dev/null 2> /dev/null").isSuccess
    }

  }

  def brokerId(host: String) = ips.indexOf(host)

  def startZookeeper: Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/zookeeper; ${startScript("./start.sh")}",
      avaiableHosts.take(3).to(ParSeq),
      false)
  }

  def startZookeeper(host: String): Unit = {
    command(s"cd ${instDirs.globalLocation}/cm-well/app/zookeeper; ${startScript("./start.sh")}", host, false)
  }

  def startZookeeper(hosts: ParSeq[String]): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/zookeeper; ${startScript("./start.sh")}",
      hosts.intersect(avaiableHosts.to(Seq)),
      false)
  }

  def stopZookeeper: Unit = stopZookeeper()

  def stopZookeeper(hosts: ParSeq[String] = ips.par, tries: Int = 5): Unit = {
    checkProduction
    //if(withZookeeper)
    killProcess("zookeeper", "", hosts, tries = tries)
  }

  def startKafka: Unit = startKafka()

  def startKafka(hosts: ParSeq[String] = ips.par): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/kafka; ${startScript("./start.sh")}", hosts, false)
  }

  def startKafka(host: String): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/kafka; ${startScript("./start.sh")}", host, false)
  }

  def stopKafka: Unit = stopKafka()

  def stopKafka(hosts: ParSeq[String] = ips.par, tries: Int = 5): Unit = {
    checkProduction
    //if(withKafka)
    killProcess("kafka.Kafka", "", hosts, tries = tries)
  }

  def startElasticsearch: Unit = startElasticsearch(ips)

  def startElasticsearch(hosts: Seq[String]): Unit

  def startCassandra: Unit = {
    startCassandra(ips.par)
  }

  def startCassandra(hosts: String*): Unit = startCassandra(hosts.par)

  def startCassandra(hosts: ParSeq[String])

  def quickInstall: Unit = {
    checkProduction
  }

  def install: Unit = install(ips)

  def install(hosts: Seq[String]) {
    val parHosts = hosts.to(ParSeq)
    checkProduction
    refreshUserState(user, None, parHosts)
    purge(parHosts)
    verifyConfigsNotChanged
    deploy(parHosts)
    init(hosts)
    //setElasticsearchUnassignedTimeout()
  }

  def disableElasticsearchUpdate: Unit = disableElasticsearchUpdate(ips(0))

  def disableElasticsearchUpdate(ip: String) {
    command(
      s"""curl -s -X PUT http://${pingAddress}:$esRegPort/_cluster/settings -d '{"transient" : {"cluster.routing.allocation.enable" : "none"}}'""",
      ip,
      false
    )
  }

  def enableElasticsearchUpdate: Unit = enableElasticsearchUpdate(ips(0))

  def enableElasticsearchUpdate(ip: String) {
    command(
      s"""curl -s -X PUT http://${pingAddress}:$esRegPort/_cluster/settings -d '{"transient" : {"cluster.routing.allocation.enable" : "all"}}'""",
      ip,
      false
    )
  }

  def findEsMasterNode(hosts: ParSeq[String] = ips.to(ParSeq)): Option[String] = {
    hosts.par.find(host => command(s"curl -s $host:$esMasterPort > /dev/null 2> /dev/null").isSuccess)
  }

  def findEsMasterNodes(hosts: ParSeq[String] = ips.to(ParSeq)): ParSeq[String] = {
    hosts.par.filter(host => command(s"curl -s $host:$esMasterPort > /dev/null 2> /dev/null").isSuccess)
  }

  def setElasticsearchUnassignedTimeout(host: String = ips.head, timeout: String = "15m"): Unit = {
    info(s"setting index.unassigned.node_left.delayed_timeout to $timeout")
    val com =
      s"""curl -s -X PUT 'http://$host:$esRegPort/_all/_settings' -d '{
         |  "settings": {
         |    "index.unassigned.node_left.delayed_timeout": "$timeout"
         |  }
         |}'""".stripMargin

    command(com, host, false)
  }

  def getCassandraHostId(addr: String): String = {
    command(
      s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath status 2> /dev/null | grep $addr | awk '{print $$7}'",
      ips(0),
      false
    ).get.trim
  }

  def shutdown: Unit = shutdown()

  def shutdown(hosts: ParSeq[String] = ips.to(ParSeq)): Unit = {
    disableElasticsearchUpdate
    stop(false, hosts)
  }

  def updateCasKeyspace: Unit = {
    // scalastyle:off
    command(s"cd ${absPath(instDirs.globalLocation)}/cm-well/app/cas/cur; sh bin/cqlsh ${pingAddress} -f ${absPath(instDirs.globalLocation)}/cm-well/conf/cas/cassandra-cql-init-cluster-new", ips(0), false)
    // scalastyle:on
  }

  def updateKafkaScemas: Unit = {
    val replicationFactor = math.min(ips.size, 3)

    val javaHomeLocation = s"${absPath(instDirs.globalLocation)}/cm-well/app/java"

    val exportCommand = s"""if [ -d $javaHomeLocation ] ;
        then export PATH=$javaHomeLocation/bin:$$PATH ;
        export JAVA_HOME=$javaHomeLocation ;
        fi ; """

    // scalastyle:off
    val createTopicCommandPrefix = s"cd ${absPath(instDirs.globalLocation)}/cm-well/app/kafka/cur; $exportCommand sh bin/kafka-topics.sh --create --zookeeper ${pingAddress}:2181 --replication-factor $replicationFactor --partitions ${ips.size} --topic"

    // scalastyle:on
    Seq("persist_topic", "persist_topic.priority", "index_topic", "index_topic.priority", "red_queue").foreach { topic =>
      command(s"$createTopicCommandPrefix $topic", ips.head, sudo = false)
    }
  }

  def checkPreUpgradeStatus(host: String): Unit = {
    val esStatusTry = elasticsearchStatus(host)
    val casStatusTry = cassandraStatus(host).map(_.toInt)
    val wsStatusTry = webServerStatus(host)

    var hasProblem = false

    esStatusTry match {
      case Success(color) =>
        if (color.toLowerCase != "green") {
          hasProblem = true
          warn(s"Elasticsearch status is $color.")
        }
      case Failure(err) =>
        hasProblem = true
        warn(s"Couldn't retrieve Elasticsearch status.")
    }

    casStatusTry match {
      case Success(uns) =>
        if (uns < casSize) {
          hasProblem = true
          warn(s"Number of Cassandra up nodes is $uns/$casSize.")
        }
      case Failure(err) =>
        hasProblem = true
        warn(s"Couldn't retrieve Cassandra status.")
    }

    wsStatusTry match {
      case Success(v) =>
        if (!v.contains("200") && !v.contains("404") && !v.contains("503")) {
          hasProblem = true
          warn(s"Webservice doesn't respond with a good code.")
        }
      case Failure(err) =>
        hasProblem = true
        warn(s"Webservice doesn't respond.")
    }

    if (hasProblem) warnPrompt
  }

  def upgradeDc = upgrade(List(DcProps(this)), uploadSpa = false, uploadDocs = false)

  def upgradeCtrl = upgrade(List(CtrlProps(this)), uploadSpa = false, uploadDocs = false)

  def upgradeBG = upgrade(List(BgProps(this)), uploadSpa = false, uploadDocs = false)

  def upgradeWS = upgrade(List(WebserviceProps(this)))

  def quickUpgrade: Unit = quickUpgrade()

  def quickUpgrade(hosts: ParSeq[String] = ips.to(ParSeq)): Unit = {
    refreshUserState(user, None, hosts)
    syncLib(hosts)
    linkLibs(hosts)
    hosts.par.foreach(host => restartApp(host))
  }

  def noDownTimeQuickUpgrade(hosts: ParSeq[String] = ips.to(ParSeq)): Unit = {
    refreshUserState(user, None, hosts)
    info("syncing libs")
    syncLib(hosts)
    linkLibs(hosts)

    info("generating resources")
    genResources(hosts)

    info("stopping CM-WELL components")
    stopBg(hosts)
    stopDc(hosts)
    stopCW(hosts)
    stopCtrl(hosts)

    val (h1, h2) = hosts.zipWithIndex.partition(x => x._2 % 2 == 0)

    val hosts1 = h1.map(_._1)
    val hosts2 = h2.map(_._1)

    info(s"restarting web services on ${hosts1.mkString(",")}")
    stopWebservice(hosts1)
    startWebservice(hosts1)
    hosts1.foreach { host =>
      info(s"waiting for $host to respond"); WebServiceLock().com(host)
    }

    info(s"restarting web services on ${hosts2.mkString(",")}")
    stopWebservice(hosts2)
    startWebservice(hosts2)

    hosts2.foreach { host =>
      info(s"waiting for $host to respond"); WebServiceLock().com(host)
    }

    startBg(hosts)
    startDc(hosts)
    startCW(hosts)
    startCtrl(hosts)
  }

  def upgrade: Unit = upgrade()

  def upgrade(baseProps: List[ComponentProps] = List(CassandraProps(this),
    ElasticsearchProps(this),
    KafkaProps(this),
    ZooKeeperProps(this),
    BgProps(this),
    WebserviceProps(this),
    CtrlProps(this),
    DcProps(this)),
              uploadSpa: Boolean = true,
              uploadDocs: Boolean = true,
              uploadUserInfotons: Boolean = true,
              withUpdateSchemas: Boolean = false,
              hosts: Seq[String] = ips,
              skipVersionCheck: Boolean = false) {
/*
    info("upgrade is disable until further notice")
    sys.exit(1)
*/

    val parHosts = hosts.to(ParSeq)
    val currentVersion = if(skipVersionCheck) Future.failed(null) else extractVersionFromProcNode(ips(0))
    //If all 3 retries will fail, will wait for result. If fails, upgrade will be stopped.
    if(!skipVersionCheck) Await.result(currentVersion, 10.seconds)

    currentVersion.map(ver => info(s"Current version is $ver"))

    checkProduction
    refreshUserState(user, None, parHosts)
    verifyConfigsNotChanged
    //checkPreUpgradeStatus(hosts(0))
    val esMasterNode = findEsMasterNode(parHosts) match {
      case Some(emn) =>
        info(s"found Elasticsearch master node: $emn")
        emn
      case None => throw new Exception("Couldn't find elasticsearch master node")

    }
    val dateStr = deployment.getCurrentDateStr
    var props = baseProps

    if (deployJava) props = props ++ List(JavaProps(this))

    info("deploying components and checking what should be upgraded.")
    syncLib(parHosts)
    linkLibs(parHosts)
    rsyncPlugins(parHosts)
    BinsProps(this).deployComponent(parHosts)

    // get for each component its unsynced hosts and redeploy the new version of the component.
    val updatedHosts = props
      .map(prop => (prop, prop.getUnsyncedHosts(hosts.par)))
      .filter(t => t._2.size > 0)
      .map(t => (t._1, t._2, t._1.redeployComponent(t._2)))
    if (updatedHosts.size > 0) {
      //todo: FIX THIS!!!
      doInfo = false
      deployment.createDirs(parHosts, props)
      doInfo = true
      val updatedComponents = updatedHosts.map(_._1).toSet
      val preUpgradeComponents = props
        .collect { case r: RunnableComponent if r.upgradeMethod == PreUpgrade => r }
        .filter(r => updatedComponents.contains(r) || !updatedComponents.intersect(r.upgradeDependency).isEmpty)
      val nonRollingComponents = props
        .collect { case r: RunnableComponent if r.upgradeMethod == NonRolling => r }
        .filter(r => updatedComponents.contains(r) || !updatedComponents.intersect(r.upgradeDependency).isEmpty)
      val rollingComponents = props
        .collect { case r: RunnableComponent if r.upgradeMethod == Rolling => r }
        .filter(r => updatedComponents.contains(r) || !updatedComponents.intersect(r.upgradeDependency).isEmpty)
      val nonRunningComponents = props.filter(p => !p.isInstanceOf[RunnableComponent])

      updatedHosts
        .filter { el =>
          nonRunningComponents.contains(el._1) && el._1.symLinkName.isDefined
        }
        .foreach { el =>
          val component = el._1
          val hostsToUpdate = el._2
          val newName = el._3
          info(s"updating ${component.getName} on all hosts")
          component.relink(newName, hostsToUpdate)
        }

      // stopping all the components that are not upgraded in rolling style.
      nonRollingComponents.foreach { nrc =>
        info(s"stopping ${nrc.getName} on all hosts.")
        nrc.stop(parHosts)
      }

      hosts.foreach { h =>
        // The components that where updated on this host.
        val updatedHostComponents =
          updatedHosts.filter(uh => uh._2.toVector.contains(h)).map(uh => uh._1 -> (uh._2, uh._3)).toMap
        val casUpdated = updatedComponents.contains(CassandraProps(this))
        val esUpdated = updatedComponents.contains(ElasticsearchProps(this))
        val javaUpdated = updatedComponents.contains(JavaProps(this))

        //if(esUpdated || javaUpdated) {
        Try(ElasticsearchLock().waitForModule(esMasterNode, esSize))
        Try(ElasticsearchStatusLock("green", "yellow").waitForModuleIndefinitely(esMasterNode))
        // if we encounter status yellow lets sleep for 10 minutes.
        //if(elasticsearchStatus(ips(0)).getOrElse("N/A") == "yellow") Thread.sleep(10 * 1000 * 60)
        //}

        info(
          s"updating ${(updatedComponents -- nonRunningComponents -- preUpgradeComponents).map(_.getName).mkString(", ")} on $h"
        )

        val updatedComponentsSet = updatedComponents
        // stopping all the components that are upgraded in rolling style.
        rollingComponents.foreach { rc =>
          info(s"  restarting ${rc.getName}")
          rc.stop(ParSeq(h))
        }

        // relinking the new components.
        (updatedComponentsSet -- preUpgradeComponents -- nonRunningComponents)
          .foreach(cp => if (cp.symLinkName.isDefined) cp.relink(updatedHostComponents.get(cp).get._2, ParSeq(h)))

        createAppLinks(ParSeq(h))
        genResources(ParSeq(h))

        // starting all the components that are upgraded in rolling style.
        rollingComponents.foreach(_.start(List(h)))

        // wait for cassandra and elasticsearch to be stable before starting cmwell components.
        if (javaUpdated || casUpdated) {
          Try(CassandraLock().waitForModule(ips(0), casSize))
        }

      }

      hosts.par.foreach(host => Try(WebServiceLock().waitForModule(host, 1)))

      preUpgradeComponents.foreach { puc =>
        info(s"restarting ${puc.getName} on all hosts")
        puc.stop(parHosts)
      }
      updatedHosts
        .filter { el =>
          preUpgradeComponents.contains(el._1) && el._1.symLinkName.isDefined
        }
        .foreach { el =>
          val component = el._1
          val hostsToUpdate = el._2
          val newName = el._3
          info(s"updating ${component.getName} on all hosts.")
          component.relink(newName, hostsToUpdate)
        }

      // todo: make more generic.
      genEsResources(parHosts)
      preUpgradeComponents.foreach(_.start(hosts))

      // starting all the components that are not upgraded in rolling style.

      Try(ElasticsearchLock(esMasterNode).waitForModule(esMasterNode, esSize))
      Try(ElasticsearchStatusLock("green", "yellow").waitForModuleIndefinitely(esMasterNode))

      if (withUpdateSchemas) {
        updateCasKeyspace
        reloadEsMappings
        updateKafkaScemas
      }

      nonRollingComponents.par.foreach { nrc =>
        info(s"starting ${nrc.getName} on all hosts.")
        nrc.start(hosts)
      }
    }

    Try(WebServiceLock().waitForModule(ips(0), 1))
    info("  waiting for ws...")
    dataInitializer.waitForWs()

    if (uploadSpa) {
      Try(WebServiceLock().waitForModule(ips(0), 1))
      info("  uploading SPAs to meta/app")
      dataInitializer.uploadDirectory("data", s"http://${hosts.head}:9000/meta/app/")
    }

    if (uploadDocs) {
      Try(WebServiceLock().waitForModule(ips(0), 1))
      info("  uploading docs")
      dataInitializer.uploadDirectory("docs", s"http://${hosts.head}:9000/meta/docs/")
    }

    if (uploadUserInfotons) {
      Try(WebServiceLock().waitForModule(ips(0), 1))
      info("  uploading basic userInfotons (if not exist)")
      dataInitializer.uploadBasicUserInfotons(hosts(0))
    }

    info("  updating version history")
    dataInitializer.logVersionUpgrade(hosts(0))

    val upgradedVersion = extractVersionFromCmwellProperties
    info(s"Upgrading to version: $upgradedVersion")

    val completed = Upgrade.runPostUpgradeActions(currentVersion, upgradedVersion, parHosts)
    completed.onComplete(_ => info(s"Upgrade completed!"))

  }

  def extractVersionFromCmwellProperties : String = {
    val cmwellProp = Source.fromURL(this.getClass.getResource(cmwellPropertiesFile)).mkString
    (Json.parse(cmwellProp) \ "cm-well_version").as[String].replace("x-SNAPSHOT", "0")
  }

  def extractVersionFromProcNode(host : String) : Future[String] = cmwell.util.concurrent.retry(3, 1.seconds){
    val procNode = SimpleHttpClient.get(s"http://${host}:9000/proc/node", Seq("format" -> "json"))
    procNode.map{r =>
      val jsonRes: JsValue = Json.parse(r.payload)
      jsonRes.\("fields").\("cm-well_version")(0).as[String]
    }

  }

  def reloadEsMappings: Unit = reloadEsMappings()

  def reloadEsMappings(createNewIndices: Boolean = true) {

    info("reloading Elasticsearch mappings")
    command(
      s"""curl -s -X POST http://${pingAddress}:$esMasterPort/_template/cmwell_index_template
         | -H "Content-Type: application/json"
         |  --data-ascii @${absPath(instDirs.globalLocation)}/cm-well/conf/es/indices_template_new.json""".stripMargin, ips(0), false)

    if (createNewIndices) {
      Thread.sleep(5000)
      createEsIndices
    }
  }

  def createEsIndices: Unit = {
    //    val numberOfShards = getSize
    //    val numberOfReplicas = 2
    //
    //    val settingsJson =
    //      s"""
    //         |{
    //         |    "settings" : {
    //         |        "index" : {
    //         |            "number_of_shards" : $numberOfShards,
    //         |            "number_of_replicas" : $numberOfReplicas
    //         |        }
    //         |    }
    //         |}
    //      """.stripMargin
    //
    //    command(s"""curl -s -XPUT 'http://${pingAddress}:$esRegPort/cm_well_0/' -d '$settingsJson'""", ips.head, false)

    //    val actionsJson =
    //      s"""
    //         |{
    //         |    "actions" : [
    //         |        {
    //         |           "add" : { "index" : "cm_well_0", "alias" : "cm_well_latest" }
    //         |        },
    //         |        {
    //         |           "add" : { "index" : "cm_well_0", "alias" : "cm_well_all" }
    //         |        }
    //         |    ]
    //         |}
    //      """.stripMargin
    //
    //
    //    command(s"""curl -s -X POST 'http://${pingAddress}:$esRegPort/_aliases' -d '$actionsJson'""", ips.head, false)
  }

  def createNewEsIndices: Unit = {
    info("creating new indices")
    val numberOfShards = getEsSize
    val numberOfReplicas = 2

    val settingsJson =
      s"""
         |{
         |    "settings" : {
         |        "index" : {
         |            "number_of_shards" : $numberOfShards,
         |            "number_of_replicas" : $numberOfReplicas
         |        }
         |    }
         |}
      """.stripMargin

    val json = command(s""" curl -s http://${ips.head}:9000/health/es""").get

    val (currents, histories) = JSON
      .parseFull(json.trim)
      .get
      .asInstanceOf[Map[String, Any]]("indices")
      .asInstanceOf[Map[String, Any]]
      .keySet
      .partition {
        _.contains("current")
      }

    val currentIndex = currents.map(_.split("_")(2).toInt).max
    val historyIndex = histories.map(_.split("_")(2).toInt).max

    val newCurrentIndex = s"cmwell_current_${currentIndex + 1}"
    val newHistoryIndex = s"cmwell_history_${historyIndex + 1}"

    val oldCurrentIndex = s"cmwell_current_$currentIndex"
    val oldHistoryIndex = s"cmwell_history_$historyIndex"

    command(s"""curl -s -XPUT 'http://${pingAddress}:$esRegPort/$newCurrentIndex/' -d '$settingsJson'""",
      ips.head,
      false)
    command(s"""curl -s -XPUT 'http://${pingAddress}:$esRegPort/$newHistoryIndex/' -d '$settingsJson'""",
      ips.head,
      false)

    val actionsJson =
      s"""
         |{
         |    "actions" : [
         |        {
         |           "add" : { "index" : "$newCurrentIndex", "alias" : "cmwell_current" }
         |        },
         |        {
         |           "add" : { "index" : "$newCurrentIndex", "alias" : "cmwell_current_latest" }
         |        },
         |        {
         |           "add" : { "index" : "$newHistoryIndex", "alias" : "cmwell_history" }
         |        },
         |        {
         |           "add" : { "index" : "$newHistoryIndex", "alias" : "cmwell_history_latest" }
         |        },
         |        {
         |           "remove" : { "index" : "$oldCurrentIndex", "alias" : "cmwell_current_latest" }
         |        },
         |        {
         |           "remove" : { "index" : "$oldHistoryIndex", "alias" : "cmwell_history_latest" }
         |        }
         |    ]
         |}
      """.stripMargin

    command(s"""curl -s -X POST 'http://${pingAddress}:$esRegPort/_aliases' -d '$actionsJson'""", ips.head, false)
  }

  def restartApp = {
    stopCtrl
    startCtrl

    Thread.sleep(5000)

    restartWebservice
    restartCW
    restartDc

    stopBg
    startBg
  }

  def restartApp(host: String) = {
    stopCtrl(host)
    startCtrl(host)

    Thread.sleep(5000)

    stopWebservice(host)
    startWebservice(host)

    stopCW(host)
    startCW(host)
    stopDc(host)
    startDc(host)

    stopBg(host)
    startBg(host)
  }

  def restartWebservice {
    ips.foreach { ip =>
      info(s"Restarting Webservice on $ip")
      stopWebservice(ParSeq(ip))
      startWebservice(ParSeq(ip))
      Try(WebServiceLock().waitForModule(ip, 1))
    }
  }

  def restartDc {
    stopDc
    startDc
  }

  def restartCW {
    stopCW
    startCW
  }

  def restartCassandra {
    ips.foreach { ip =>
      info(s"Restarting Cassandra on $ip")
      stopCassandra(ParSeq(ip))
      startCassandra(ParSeq(ip))
      Try(CassandraLock().waitForModule(ips(0), casSize))
    }
  }

  def restartElasticsearch: Unit = restartElasticsearch(ips)

  def restartElasticsearch(hosts: Seq[String]) {
    hosts.foreach { host =>
      Try(ElasticsearchStatusLock("green").waitForModule(hosts(0), 1000))
      info(s"Restarting Elasticsearch on $host")
      disableElasticsearchUpdate(ips((ips.indexOf(host) + 1) % ips.size))
      Thread.sleep(10000)
      stopElasticsearch(ParSeq(host))
      startElasticsearch(Seq(host))
      enableElasticsearchUpdate(ips((ips.indexOf(host) + 1) % ips.size))
    }
  }

  /*def findIpToConnectWithToGrid : String = {
    Await.result(Future.firstCompletedOf(ips map getIpInGrid), Duration.Inf)
  }

  def getIpInGrid(ipToCheckAgainst : String) : Future[String] = {
    import java.net.NetworkInterface
    import java.util
    import collection.JavaConversions._
    import collection.JavaConverters._
    Future {
      val interfaces: Seq[java.net.NetworkInterface] = util.Collections.list(NetworkInterface.getNetworkInterfaces())
      val validInterfaceOpt = interfaces.collectFirst { case i if (command(s"ping -c 1 -I ${i.getName}
       $ipToCheckAgainst ; echo $$?").get.split("\n").toList.last.trim == "0") => i}
      validInterfaceOpt match {
        case Some(validInterface) =>
          validInterface.getInterfaceAddresses.asScala.collectFirst {
            case inetAddr if (inetAddr.getAddress.getHostAddress.matches( """\d+.\d+.\d+.\d+""")) =>
              inetAddr.getAddress.getHostAddress
          }.get
      }
    }
  }*/

  def findIpToConnectWithToGrid: String = {
    var out = Option.empty[String]
    ips.find { ip =>
      val res = getIpInGrid(ip)
      val rv = res.isDefined
      if(rv) out = Some(res.get)
      rv
    }
    out.getOrElse(ips(0))
  }

  def getIpInGrid(ipToCheckAgainst: String): Option[String] = {
    import java.net.NetworkInterface
    import java.util

    import scala.collection.JavaConverters._

    val interfaces: Seq[java.net.NetworkInterface] = util.Collections.list(NetworkInterface.getNetworkInterfaces()).asScala.toSeq
    val validInterfaceOpt = interfaces.collectFirst {
      case i
        if (command(s"ping -c 1 -I ${i.getName} $ipToCheckAgainst ; echo $$?").get
          .split("\n")
          .toList
          .last
          .trim == "0") =>
        i
    }
    validInterfaceOpt match {
      case Some(validInterface) =>
        validInterface.getInterfaceAddresses.asScala.collectFirst {
          case inetAddr if (inetAddr.getAddress.getHostAddress.matches("""\d+.\d+.\d+.\d+""")) =>
            inetAddr.getAddress.getHostAddress
        }
      case None => None
    }
  }

  def connectToGrid: Unit = connectToGrid()

  def connectToGrid(ip: String = "") {
    if (!Host.connectedToAkkaGrid) {
      val useIp = if (ip == "") {
        val uIp = findIpToConnectWithToGrid
        info(s"Connecting to grid with ip: $uIp")
        uIp
      } else ip
      AkkaGrid.setGridConnection(
        GridConnection(memberName = "CONS",
          clusterName = cn,
          hostName = useIp,
          port = 0,
          seeds = ips.take(3).map(seedIp => s"$seedIp:7777").to(Set))
      )
      AkkaGrid.joinClient

      CtrlClient.init(ips(0))
      Host.connectedToAkkaGrid = true
      Thread.sleep(5000)
    }
  }

  def restartHaproxy(sudoer: Credentials) {
    haProxy match {
      case Some(HaProxy(host, sitedown)) =>
        command("sudo service haproxy restart", ParSeq(host), true, Some(sudoer))
      case None =>
    }
  }

  def stopHaproxy(sudoer: Credentials) {
    haProxy match {
      case Some(HaProxy(host, sitedown)) =>
        command("sudo service haproxy stop", ParSeq(host), true, Some(sudoer))
      case None =>
    }
  }

  def deployHaproxy(sudoer: Credentials) {
    throw new Exception("deploy haproxy currently cancelled")
    /*
    haProxy match {
      case Some(HaProxy(host, sitedown)) =>
        command("sudo apt-get -q -y install haproxy", Seq(host), true, Some(sudoer))
        val servers = ips.map(ip => s"""server  $ip $ip:9000 check inter 10000 rise 5 fall 3""").mkString("\n")
        val content = ResourceBuilder.getResource("scripts/templates/haproxy.cfg", Map("cluster" -> cn, "sitedown" -> sitedown, "servers" -> servers))
        createFile("/etc/haproxy/haproxy.cfg", content, Seq(host), true, Some(sudoer))
        restartHaproxy(sudoer)
      case None =>
    }
   */
  }

  def getClusterStatus: ClusterStatus = {
    connectToGrid
    Await.result(Host.ctrl.getClusterStatus, 30 seconds)
  }

  def syncLib(hosts: ParSeq[String] = ips.to(ParSeq)) = {
    def getCurrentDateStr = {
      val format = new java.text.SimpleDateFormat("yyyyMMdd_hhmmss")
      val date = new Date()
      format.format(date)
    }

    val currentDate = getCurrentDateStr

    hosts.foreach { host =>
      val comStr =
        s"""test -L ${instDirs.globalLocation}/cm-well/lib &&
           |cp -al `readlink ${instDirs.globalLocation}/cm-well/lib`/ ${instDirs.globalLocation}/cm-well/lib-$currentDate/ ||
           |mkdir -p ${instDirs.globalLocation}/cm-well/lib-$currentDate""".stripMargin

      command(comStr, host, false)

      command(s"test -L ${instDirs.globalLocation}/cm-well/lib && rm ${instDirs.globalLocation}/cm-well/lib",
        host,
        false)
      command(s"ln -s ${instDirs.globalLocation}/cm-well/lib-$currentDate ${instDirs.globalLocation}/cm-well/lib",
        host,
        false)

      rsync("lib/", s"${instDirs.globalLocation}/cm-well/lib/", ParSeq(host))
    }

  }

  def linkLibs(hosts: ParSeq[String] = ips.par) = {
    val dir = new File("dependencies")

    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/dependencies", hosts, false)
    rsync(s"dependencies/", s"${instDirs.globalLocation}/cm-well/dependencies/", hosts)

    dir.listFiles().toVector.par.foreach { file =>
      linkLib(file.getName, hosts)
    }
  }

  def linkLib(component: String, hosts: ParSeq[String] = ips.to(ParSeq)) = {

    val target = component //if(component == "batch") "bg" else component

    //val content = Source.fromFile(s"dependencies/$component").getLines().toVector

    command(s"rm ${instDirs.globalLocation}/cm-well/app/$target/lib/* > /dev/null 2> /dev/null", hosts, false)
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/app/$target/lib", hosts, false)

    hosts.foreach { host =>
      // scalastyle:off
      command(s"cat ${instDirs.globalLocation}/cm-well/dependencies/$component | xargs -I zzz ln -s ${instDirs.globalLocation}/cm-well/lib/zzz ${instDirs.globalLocation}/cm-well/app/$target/lib/zzz", host, false)
      // scalastyle:on
    }
  }

  sys.addShutdownHook {
    Try(k.grid.Grid.shutdown)
  }

  def rsyncPlugins(hosts: ParSeq[String] = ips.to(ParSeq)) = {
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/app/ws/plugins/sg-engines/", hosts, false)
    rsync(s"plugins/", s"${instDirs.globalLocation}/cm-well/app/ws/plugins/sg-engines/", hosts)
  }
}
