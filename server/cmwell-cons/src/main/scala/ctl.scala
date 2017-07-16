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


//import scala.sys.process.processInternal.File

// comment

import java.io.{ByteArrayInputStream, File, PrintWriter}
import java.nio.file.Files
import java.util.Date

import cmwell.ctrl.client.CtrlClient
import cmwell.ctrl.hc.{ActiveNodes, ClusterStatus}
import cmwell.util.build.BuildInfo
import k.grid.{GridConnection, Grid => AkkaGrid}
import org.apache.commons.io.FileUtils

import scala.collection.{GenSeq, GenSet}
import scala.collection.parallel.{ParMap, ParSeq}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
import scala.util.parsing.json._
import scala.util.{Failure, Success, Try}
//todo: make sure that some applications are installed.

trait Info {
  def info(msg: String) = println(s"Info: $msg")
}


object ResourceBuilder {
  def getIndexedName(name: String, index: Int): String = {
    index match {
      case 1 => name
      case _ => s"$name$index"
    }
  }

  private def replaceTemplates(text: String, templates: Map[String, String]): String =
    """\{\{([^{}]*)\}\}""".r replaceSomeIn(text, {
      case scala.util.matching.Regex.Groups(name) => templates get name
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
    println("failed to check " + name)
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

case class BatchResult(name: String, indexed: BigInt, fileSize: BigInt)

case class BatchQuery(name: String, head: String, log: String)

case class HostBatchStatus(name: String, br: ParSeq[BatchResult])

//List("/mnt/d1/cas", "/mnt/d1/cas2", "/mnt/d1/cas3", "/mnt/d1/cas4")


case class DataDirs(casDataDirs: GenSeq[String],
                    casCommitLogDirs: GenSeq[String],
                    esDataDirs: GenSeq[String],
                    tlogDataDirs: GenSeq[String],
                    kafkaDataDir: String,
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
                    ipMappings: IpMappings,
                    size: Int,
                    inet: String,
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
                    withElk: Boolean = false,
                    val withZkKfk: Boolean = false,
                    val withOldBg: Boolean = true,
                    isDebug:Boolean = false) {

  var sudoerCredentials: Option[Credentials] = None

  def getUser = user

  def getIpMappings = ipMappings

  def getInet = inet

  def getDataDirs = dataDirs

  def getInstDirs = instDirs

  def getAllocationPlan = allocationPlan

  def getUseAuthorization = useAuthorization

  def getDeployJava = deployJava

  def getProduction = production

  def getSu = su

  def getCtrlSerice = ctrlService

  def getHaProxy = haProxy

  def getWithElk = withElk

  /*
    var useAuthorization = false
    var deployJava = false
    var production = false
    var devMode = false
  */

  def getMinMembers = minMembers.getOrElse(ips.size / 2 + 1)

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
      case str: String => Oracle
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
      ip -> command(s"ps aux | grep java | egrep -v 'starter|grep' | awk '{print $$2}' | xargs -I zzz ${getInstDirs.globalLocation}/cm-well/app/java/bin/jstat -gcutil zzz", ip, false).map(_.trim)
    }.toMap
  }

  def jstat(comp: String) = {
    ips.par.map { ip =>
      ip -> command(s"ps aux | grep java | egrep -v 'starter|grep' | grep $comp | awk '{print $$2}' | xargs -I zzz ${getInstDirs.globalLocation}/cm-well/app/java/bin/jstat -gcutil zzz", ip, false).map(_.trim)
    }.toMap
  }

  def jstat(comp: String, ip: String): Unit = {
    ParMap(ip -> command(s"ps aux | grep java | egrep -v 'starter|grep' | grep $comp | awk '{print $$2}' | xargs -I zzz ${getInstDirs.globalLocation}/cm-well/app/java/bin/jstat -gcutil zzz", ip, false).map(_.trim))
  }

  private val componentToJmxMapping = Map(
    "ws" -> PortManagers.ws.jmxPortManager.initialPort,
    "batch" -> PortManagers.batch.jmxPortManager.initialPort,
    "ctrl" -> PortManagers.ctrl.jmxPortManager.initialPort,
    "dc" -> PortManagers.dc.jmxPortManager.initialPort
  )

  def jconsole(component: String, dualmonitor: Boolean, host1: String, hosts: String*): Unit = jconsole(component, dualmonitor, Seq(host1) ++ hosts)

  def jconsole(component: String, dualmonitor: Boolean = false, hosts: GenSeq[String] = ips): Unit = {

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
    command(s"""curl -XPOST "http://${ips(0)}:9000/meta/sys/dc/$dc" -H "X-CM-Well-Type:Obj" -H "Content-Type:application/json" --data-binary '{"type":"remote" , "location" : "$remoteHost"  , "id" : "$dc"}'""")
  }

  def ips = ipMappings.getIps

  def getSize = size


  def createFile(path: String, content: String, hosts: GenSeq[String] = ips, sudo: Boolean = false, sudoer: Option[Credentials] = None) {
    if (sudo)
      command(s"""echo -e '$content' | sudo tee $path > /dev/null""", hosts, true, sudoer)
    else
      command(s"""echo $$'$content' > $path""", hosts, false)
  }

  val shipperConfLocation = s"${instDirs.globalLocation}/cm-well/conf/logstash"
  val logstashJarLocation = s"${instDirs.globalLocation}/cm-well/app/logstash"
  val logstashConfName = "logstash.conf"
  val logstashJarName = "logstash-1.2.2-flatjar.jar"

  def addLogstash(esHost: String, hosts: GenSeq[String] = ips) {
    createLogstashConfFile(esHost, hosts)
    deployLogstash(hosts)
    startSendingLogsToLogstash(hosts)
  }

  def createLogstashConfFile(esHost: String, hosts: GenSeq[String] = ips) {
    val str = genLogstashConfFile(esHost, Map("BU" -> "TMS", "serviceID" -> "cm-well", "environmentID" -> "cm-well", "appID" -> "cm-well", "cluster" -> cn))
    command(s"mkdir -p $shipperConfLocation", hosts, false)
    createFile(s"$shipperConfLocation/$logstashConfName", str, hosts)
  }

  def deployLogstash(hosts: GenSeq[String] = ips) {
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/app/logstash", hosts, false)
    rsync(s"components-extras/$logstashJarName", logstashJarLocation, hosts)
    val startFile = s"java -jar $logstashJarName agent -f $shipperConfLocation/$logstashConfName > /dev/null 2> /dev/null &"
    createFile(s"$logstashJarLocation/start.sh", startFile, hosts)
    command(s"cd $logstashJarLocation; chmod +x start.sh", hosts, false)
  }

  def startSendingLogsToLogstash(hosts: GenSeq[String] = ips) {
    command(s"cd $logstashJarLocation; ./start.sh", hosts, false)
  }

  def stopSendingLogsToLogstash(hosts: GenSeq[String] = ips) {
    killProcess("logstash", "")
  }

  def genLogstashConfFile(esHost: String, globalFields: Map[String, String]): String = LogstashConf.genLogstashConfFile(cn, esHost, globalFields, s"${instDirs.globalLocation}/cm-well/log", dataDirs.esDataDirs.size)


  private def resolveIndex(index: Int): String = {
    index match {
      case 1 => ""
      case _ => s"${index}"
    }
  }

  object tests {
    def loadTest(port: Int = 9000, host: String = ips(0), amount: Int = 1000000, path: String = "/test/lorem") {
      val com = Seq("java", "-jar", "components/lorem-ipsum-agent-executable.jar", "-hosts", host, "-p", path, "-a", "5", "-v", "3", "-n", s"$amount", "--port", s"$port").run
      val startTime: Long = System.currentTimeMillis / 1000

      var currentAmount = 0

      while (currentAmount < amount) {
        currentAmount = (Seq("curl", "-s", s"http://$host:$port$path?format=atom&op=search&length=0") #| Seq("grep", "-o", "<os:totalResults>[0-9]*</os:totalResults>") #| Seq("grep", "-o", "[0-9]*") !!).trim.toInt
        val timePassed = (System.currentTimeMillis / 1000 - startTime)
        println(s"infotons uploaded: $currentAmount, time passed: ${Math.round(timePassed / 60)}:${timePassed % 60}")
        Thread.sleep(5000)
      }
    }

    def printBatchStatus {
      val results = batchStatus
      results.toList.foreach {
        hostRes =>
          println(s"---------------------------------------${hostRes.name}---------------------------------------")
          hostRes.br.toList.foreach {
            res =>
              println(res.name)
              if (res.indexed == res.fileSize) {
                println("Nothing to proccess")
              } else {
                println(s"${res.fileSize - res.indexed} bytes to index")
              }
          }
      }
    }


    def batchStatus: ParSeq[HostBatchStatus] = {
      val batchQuries = List(BatchQuery("Imp", "imp_UpdateTLog", "UpdateTLog_updatesPar"), BatchQuery("Indexer", "indexer_UuidsTLogupdatesPar", "UuidsTLog_uuidsPar"))

      val y = ips.par.map {
        host =>
          val res = batchQuries.par.map { query =>
            //val res = command( """cd """ + instDirs.globalLocation + """/cm-well/data/tlog/data/tlog ; cat """ + query.head +  """*; printf "," ;ls -alF | grep """ + query.log +  """ | awk '{print $5}'""", host, false)
            val res = command( """cd """ + instDirs.globalLocation + """/cm-well/data/tlog ; cat """ + query.head + """*; printf "," ;ls -alF | grep """ + query.log + """ | awk '{print $5}'""", host, false)
            val x = res match {
              case Success(str) =>

                val t = Try {
                  val vals = str.split("\n")(0).split(",")
                  BatchResult(query.name, BigInt(vals(0)), BigInt(vals(1)))
                }

                t match {
                  case Success(br) => br
                  case Failure(ex) => BatchResult(s"${query.name} - problem!!", BigInt(1), BigInt(0))
                }

              case Failure(ex) =>
                throw new Exception("can't execute command on this host")
            }
            x
          }
          HostBatchStatus(host, res)
      }
      y
    }

    def infotonChangeAvg: BigDecimal = infotonChangeAvg()

    def infotonChangeAvg(checkCount: Int = 5): BigDecimal = {
      val checks = (1 to checkCount).toList
      val res = checks.map { x => Thread.sleep(1000); infotonCount }

      val x = (res.drop(1) zip res.dropRight(1)).map { item => item._1 - item._2 }
      val y = x.foldRight[BigDecimal](0)(_ + _) / checkCount
      y
    }

    def infotonCount: BigDecimal = {
      val res = command(s"curl -s  ${pingAddress}:$esRegPort/_status", ips(0), false).get
      val json = JSON.parseFull(res).get.asInstanceOf[Map[String, Any]]
      BigDecimal(json("indices").asInstanceOf[Map[String, Any]]("cmwell0_current").asInstanceOf[Map[String, Any]]("docs").asInstanceOf[Map[String, Any]]("num_docs").toString)
    }

    def getCpuUsage: List[Double] = {
      // top -bn 2 -d 0.01 | grep '^%Cpu' | tail -n 1 | awk '{print $2+$4+$6}'
      val ret1 = ips.par.map {
        ip =>
          val res = command("top -bn 2 -d 0.01 | grep '^%Cpu' | tail -n 1 | awk '{print $2+$4+$6}'", ip, false)
          val ret2 = res match {
            case Success(str) => str.toDouble
            case Failure(err) => -1.0d
          }
          ret2
      }
      ret1.toList
    }
  }

  val deployment = new Deployment(this)

  //FIXME: was object inside a class, caused this nifty hard to track exception:
  /*
   * java.lang.NoSuchFieldError: LogLevel$module
   *	at Host.LogLevel(ctl.scala:415)
   *	at Main$$anon$1.<init>(scalacmd4970030751979185144.scala:10)
   *	at Main$.main(scalacmd4970030751979185144.scala:1)
   *	at Main.main(scalacmd4970030751979185144.scala)
   *	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   *	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
   *	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   *	at java.lang.reflect.Method.invoke(Method.java:497)
   *	at scala.reflect.internal.util.ScalaClassLoader$$anonfun$run$1.apply(ScalaClassLoader.scala:70)
   *	at scala.reflect.internal.util.ScalaClassLoader$class.asContext(ScalaClassLoader.scala:31)
   *	at scala.reflect.internal.util.ScalaClassLoader$URLClassLoader.asContext(ScalaClassLoader.scala:101)
   *	at scala.reflect.internal.util.ScalaClassLoader$class.run(ScalaClassLoader.scala:70)
   *	at scala.reflect.internal.util.ScalaClassLoader$URLClassLoader.run(ScalaClassLoader.scala:101)
   *	at scala.tools.nsc.CommonRunner$class.run(ObjectRunner.scala:22)
   *	at scala.tools.nsc.ObjectRunner$.run(ObjectRunner.scala:39)
   *	at scala.tools.nsc.CommonRunner$class.runAndCatch(ObjectRunner.scala:29)
   *	at scala.tools.nsc.ObjectRunner$.runAndCatch(ObjectRunner.scala:39)
   *	at scala.tools.nsc.ScriptRunner.scala$tools$nsc$ScriptRunner$$runCompiled(ScriptRunner.scala:175)
   *	at scala.tools.nsc.ScriptRunner$$anonfun$runCommand$1.apply(ScriptRunner.scala:222)
   *	at scala.tools.nsc.ScriptRunner$$anonfun$runCommand$1.apply(ScriptRunner.scala:222)
   *	at scala.tools.nsc.ScriptRunner$$anonfun$withCompiledScript$1$$anonfun$apply$mcZ$sp$1.apply(ScriptRunner.scala:161)
   *	at scala.tools.nsc.ScriptRunner$$anonfun$withCompiledScript$1.apply$mcZ$sp(ScriptRunner.scala:161)
   *	at scala.tools.nsc.ScriptRunner$$anonfun$withCompiledScript$1.apply(ScriptRunner.scala:129)
   *	at scala.tools.nsc.ScriptRunner$$anonfun$withCompiledScript$1.apply(ScriptRunner.scala:129)
   *	at scala.tools.nsc.util.package$.trackingThreads(package.scala:43)
   *	at scala.tools.nsc.util.package$.waitingForThreads(package.scala:27)
   *	at scala.tools.nsc.ScriptRunner.withCompiledScript(ScriptRunner.scala:128)
   *	at scala.tools.nsc.ScriptRunner.runCommand(ScriptRunner.scala:222)
   *	at scala.tools.nsc.MainGenericRunner.run$1(MainGenericRunner.scala:85)
   *	at scala.tools.nsc.MainGenericRunner.process(MainGenericRunner.scala:98)
   *	at scala.tools.nsc.MainGenericRunner$.main(MainGenericRunner.scala:103)
   *	at scala.tools.nsc.MainGenericRunner.main(MainGenericRunner.scala)
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

  private val jwt = sys.env.getOrElse("PUSER_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwVXNlciIsImV4cCI6NDYzODkwMjQwMDAwMCwicmV2IjoxfQ.j-tJCGnWHbJ-XAUJ1wyHxMlnMaLvO6IO0fKVjsXOzYM")
  private val rootDigest = sys.env.getOrElse("ROOT_DIGEST", "$2a$10$MKrHtymBOyfE67dZnbEdeOEB336uOXwYetVU28djINKjUTs2da6Km")
  private val rootDigest2 = sys.env.getOrElse("ROOT_DIGEST2", "199245447fd82dd38f84c000da94cf1d")

  var verbose = false

  var deb = isDebug

  def debug: Boolean = deb

  def debug_=(v: Boolean) = {
    deb = v
    println("The ports are:\nws: 5010\nbatch: 5009\nctrl: 5011\ncw: 5012\ndc: 5013\nbg: 5014")
  }


  var doInfo = true

  def info(msg: String) = if (doInfo) println(s"Info: $msg")

  def warn(msg: String) = {
    println(s"Warning: $msg")
  }

  def warnPrompt = {
    println("Warning: Are you sure you want to continue: (yes/no)")
    val ln = scala.io.StdIn.readLine()
    if (ln != "yes") {
      throw new Exception("You chose to not continue the process.")
    }
  }

  def getMode: String

  def getCassandraHostIDs(host: String): String

  def getElasticsearchMasters: Int

  def isSu = su

  def help = println(Source.fromFile("readme").mkString)

  //def hosts = ips.map(ip => s"${user}@${ip}")
  def getSeedNodes: List[String]


  def javaPath = s"${instDirs.globalLocation}/cm-well/app/java/bin"

  def utilsPath = s"${instDirs.globalLocation}/cm-well/bin/utils"

  def homeBinPath = "~/bin"

  def path: String = s"$javaPath:$utilsPath:$homeBinPath:$$PATH"

  private def ipsToSsh(u: String = user, ips: GenSeq[String]) = ips.map(ip => if (ip.indexOf("@") == -1) s"${u}@${ip}" else ip)

  private def timeStamp = System.currentTimeMillis / 1000

  private var lastProdCheckTimeStamp = 0L

  private def checkProduction {
    val interval = 60 * 60


    if (production && (timeStamp - lastProdCheckTimeStamp > interval)) {
      println("This is a production cluster. Are you sure you want to do this operation: (yes/no)")
      val ln = scala.io.StdIn.readLine()

      if (ln != "yes") {
        throw new Exception("This operation is not allowed on a production environment. Please remove: production = true from this cluster's definition file")
      } else {
        lastProdCheckTimeStamp = timeStamp
      }
    }
  }

  //  var intallationDir = "~/cm-well-new/cm-well"
  //  var globalLocation = "/opt"

  case class Credentials(name: String, pass: String)

  def gainTrust: Unit = gainTrust()

  def gainTrust(u: String = user, p: String = "", hosts: GenSeq[String] = ips) {
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
    sshHosts.seq.foreach { sshHost => Seq("ssh-keygen", "-R", sshHost).!! }
    sshHosts.foreach { sshHost =>
      val cmd = Seq("bash", "-c", s"read PASS; ${UtilCommands.sshpass} -p $$PASS ssh-copy-id -i $privateKey -o StrictHostKeyChecking=no $sshHost")
      if (verbose) println("command: " + cmd.mkString(" "))
      (s"echo -e -n $pass\\n" #| cmd).!!
    }
  }

  def refreshUserState(user: String, sudoer: Option[Credentials], hosts: GenSeq[String] = ips): Unit = {
    // temp disabled for OSX till new cons available...
    val pubKeyOpt = sys.env.get("SSH_DEV_KEY")
    if(!UtilCommands.isOSX && pubKeyOpt.isDefined) {
      val pubKey = pubKeyOpt.get
      val userSshDir = s"/home/$user/.ssh"
      val rootSshDir = "/root/.ssh"
      val fileName = "authorized_keys"
      val rootVarMap = Map("STR" -> pubKey, "DIR" -> rootSshDir, "FILE" -> fileName)
      val cmdTemplate = "%smkdir -p $DIR; %ssed -i -e '\\$a\\' $DIR/$FILE 2> /dev/null; %sgrep -q '$STR' $DIR/$FILE 2> /dev/null || echo -e '$STR' | %stee -a $DIR/$FILE > /dev/null"
      val rootCmd = cmdTemplate.format(Seq.fill(4)("sudo "): _ *)
      val userCmd = cmdTemplate.format(Seq.fill(4)(""): _ *)
      sudoer.foreach(_ => command(rootCmd, hosts, sudo = true, sudoer, rootVarMap))
      val userVarMap = Map("STR" -> pubKey, "DIR" -> userSshDir, "FILE" -> fileName)
      command(userCmd, hosts, sudo = false, sudoer = None, variables = userVarMap)
      //add the file that removes the annoying ssh log in message
      command("touch ~/.hushlogin", hosts, sudo = false)
    }
  }

  //def gainTrustNoPass(u : String = user , p : String = "", hosts : GenSeq[String] = ips.par)

  def validateNumberOfMasterNodes(num: Int, size: Int): Boolean = (num % 2) == 1 && num <= size && num >= 3

  def absPath(path: String) = Seq("bash", "-c", s"cd ${path}; pwd").!!.replace("\n", "")

  val nodeToolLocation = s"${instDirs.globalLocation}/cm-well/app/cas/cur/bin/nodetool"

  def nodeToolPath = nodeToolLocation

  def pingAddress = ips(0)

  def esHealthAddress = ":9200/_cluster/health?pretty=true"

  var mappingFile = "mapping.json"


  def cassandraStatus(host: String): Try[String] = {
    command(s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath status 2> /dev/null | grep UN | wc -l", host, false).map(_.trim)
  }

  case class CassandraLock() extends ModuleLock {
    def name: String = "Cassandra boot"

    def com(host: String): Try[String] = cassandraStatus(host)

    def continueCondition(v: String, waitFor: Int): Boolean = v.toInt < waitFor
  }

  case class CassandraDNLock() extends ModuleLock {
    def name: String = "CassandraDownNodes counter"

    def com(host: String): Try[String] = command(s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath status 2> /dev/null | grep DN | wc -l", host, false)

    def continueCondition(v: String, waitFor: Int): Boolean = v.toInt < waitFor
  }

  case class ElasticsearchLock(checkHost: String = ips(0)) extends ModuleLock {
    def name: String = "Elasticsearch boot"

    def com(host: String): Try[String] = {
      val r = command("curl -s GET http://" + checkHost + esHealthAddress, host, false)
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

    override def com(host: String): Try[String] = command("curl -s GET http://" + host + ":9000/meta/sys?format=ntriples")
  }

  def webServerStatus(host: String) = command("curl -Is GET http://" + host + ":9000/ | head -1")

  case class WebServiceLock() extends ModuleLock {
    def name: String = "Web Service boot"

    def com(host: String): Try[String] = webServerStatus(host)

    def continueCondition(v: String, waitFor: Int): Boolean = !v.contains("200") && !v.contains("404") && !v.contains("503")
  }

  val dataInitializer = new DataInitializer(this, jwt, rootDigest, rootDigest2)

  def shutDownDataInitializer() = dataInitializer.shutdownMaterializerAndActorSystem()

  implicit class StringExtensions(s: String) {
    def takeRightWhile(p: (Char) => Boolean): String = s.takeRight(s.reverseIterator.takeWhile(p).length)
  }

  def createCassandraRackProperties(hosts: GenSeq[String] = ips.par) {
    hosts.zipWithIndex.foreach {
      ip =>
        val content = s"dc=DC1\nrack=RAC${ip._2 + 1}"
        command(s"""echo "$content" > ${instDirs.globalLocation}/cm-well/conf/cas/cassandra-rackdc.properties""", ip._1, false)
        for (i <- 2 to dataDirs.casDataDirs.size)
          command(s"""echo "$content" > ${instDirs.globalLocation}/cm-well/conf/cas$i/cassandra-rackdc.properties""", ip._1, false)
    }
  }


  def createUser(user: String = "u", pass: String = "said2000", hosts: GenSeq[String] = ips.par, sudoer: Credentials) {
    command(s"sudo useradd $user", hosts, true, Some(sudoer))
    command(s"echo '$user:$$USERPASS' | sudo chpasswd", hosts, true, Some(sudoer), Map("USERPASS" -> pass))
  }

  def sudoComm(com: String) = s"""sudo bash -c \"\"\"${com}\"\"\""""

  def elasticsearchStatus(host: String) = {
    val r = command("curl -s GET http://" + ips(0) + esHealthAddress, host, false)
    r match {
      case Success(v) =>
        Try(JSON.parseFull(v.trim).get.asInstanceOf[Map[String, Any]]("status").toString.trim.split('.')(0))
      case Failure(e) => Failure(e)
    }
  }

  def command(com: String, hosts: GenSeq[String], sudo: Boolean): GenSeq[Try[String]] = {
    command(com, hosts, sudo, None)
  }

  def command(com: String, hosts: GenSeq[String], sudo: Boolean, sudoer: Option[Credentials]): GenSeq[Try[String]] = {
    hosts.map {
      host =>
        command(com, host, sudo, sudoer)
    }
  }

  def command(com: String, hosts: GenSeq[String], sudo: Boolean, sudoer: Option[Credentials], variables: Map[String, String]): GenSeq[Try[String]] = {
    hosts.map(host => command(com, host, sudo, sudoer, variables))
  }

  def command(com: String, host: String, sudo: Boolean): Try[String] = {
    command(com, host, sudo, None)
  }

  def command(com: String, host: String, sudo: Boolean, sudoer: Option[Credentials]): Try[String] = {
    command(com, host, sudo, sudoer, Map[String, String]())
  }

  def command(com: String, host: String, sudo: Boolean, sudoer: Option[Credentials], variables: Map[String, String]): Try[String] = {
    if (sudo && isSu && sudoer.isEmpty) throw new Exception(s"Sudoer credentials must be available in order to use sudo")
    if (!ips.contains(host) && host != haProxy.map(x => x.host).getOrElse("")) throw new Exception(s"The host $host is not part of this cluster")
    val (readVarsLine, varValues) = variables.fold(("", "")) {
      case ((readVarsStr, varValuesForEcho), (varName, value)) =>
        (s"$readVarsStr read $varName;", s"$varValuesForEcho$value\\n")
    }
    val (commandLine, process) = if (sudo && isSu) {
      val cmd = s"""ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR ${sudoer.get.name}@$host export PATH=$path;$readVarsLine read PASS; sshpass -p $$PASS bash -c "${escapedCommand(com)}"""" //old version that get stuck sometimes - val command = s"""ssh -o StrictHostKeyChecking=no ${sudoer.get.name}@$host bash -c $$'{ export PATH=$path; read PASS; ./sshpass -p $$PASS bash -c "${escapedCommand(com)}"; }'"""
      (cmd, s"echo -e -n $varValues${sudoer.get.pass}\\n" #| cmd)
    } else {
      if (variables.nonEmpty) {
        val cmd = s"""ssh -o StrictHostKeyChecking=no -o LogLevel=ERROR $user@$host export PATH=$path;$readVarsLine bash -c "${escapedCommand(com)}""""
        (cmd, s"echo -e -n $varValues" #| cmd)
      }
      else {
        val cmd = Seq("ssh", "-o", "StrictHostKeyChecking=no", "-o", "LogLevel=ERROR", s"$user@$host", s"PATH=$path $com")
        (cmd.mkString(" "), Process(cmd))
      }
    }
    if (verbose) println("command: " + commandLine)
    Try(process.!!)
  }

  private def escapedCommand(cmd: String) = cmd.replace("\"", "\\\"") // old version for $'..' bash string: cmd.replace("\"", "\\\\\"").replace("'", "\\'")

  def command(com: String, sudo: Boolean = false): Try[String] = {
    if (sudo && isSu)
      Try(sudoComm(com).!!)
    else {
      val seq = Seq("bash", "-c", com)
      if (verbose) println("command: " + seq.mkString(" "))
      Try(seq.!!)
    }
  }

  def rsync(from: String, to: String, hosts: GenSeq[String], sudo: Boolean = false): GenSeq[Try[String]] = {
    val h = hosts.map(host => if (host.indexOf("@") == -1) s"${user}@${host}" else host)
    h.map {
      host =>
        _rsync(from, to, host, sudo = sudo)
    }
  }

  def _rsync(from: String, to: String, host: String, tries: Int = 10, sudo: Boolean): Try[String] = {
    val seq = Seq("rsync", "-Paz", "--delete", from, host + ":" + to)

    if (verbose) println("command: " + seq.mkString(" "))
    val res = Try(seq.!!)
    res match {
      case Success(r) => res
      case Failure(err) => if (tries == 0) res else _rsync(from, to, host, tries - 1, sudo)
    }
  }


  def removeDataDirs: Unit = removeDataDirs()

  def removeDataDirs(i: GenSeq[String] = ips.par) {

    command(s"rm -rf ${instDirs.intallationDir}", i, false)

    dataDirs.casDataDirs.foreach {
      cas => command(s"rm -rf ${cas}", i, false)
    }

    dataDirs.casCommitLogDirs.foreach {
      ccl => command(s"rm -rf ${ccl}", i, false)
    }

    dataDirs.esDataDirs.foreach {
      es => command(s"rm -rf ${es}", i, false)
    }

    dataDirs.tlogDataDirs.foreach {
      tlog =>
        command(s"rm -rf $tlog", i, false)
    }


    command(s"rm -rf ${dataDirs.logsDataDir}", i, false)
  }

  def createDataDirs(): Unit = createDataDirs(ips.par)

  def createDataDirs(hosts: GenSeq[String]) {
    info("creating data directories")

    info("  creating installation directory")
    command(s"mkdir -p ${instDirs.intallationDir}/", hosts, false)
    deployment.componentProps.collect { case cp: DataComponent => cp } foreach {
      _.createDataDirectories(hosts)
    }

    info("  creating log data directory")
    command(s"mkdir -p ${dataDirs.logsDataDir}", hosts, false)

    info("finished creating data directories")
  }

  def deployComponents(hosts: GenSeq[String] = ips.par) {
    deployment.componentProps.foreach(_.deployComponent(hosts = hosts))
  }

  def genResources(hosts: GenSeq[String] = ips) {
    deployment.createResources(mkScripts(hosts))
  }


  def genEsResources(hosts: GenSeq[String]) {
    deployment.createResources(mkScripts(hosts).filter(_.isInstanceOf[ElasticsearchConf]))
  }


  def genCtrlResources(hosts: GenSeq[String]) {
    deployment.createResources(mkScripts(hosts).filter(_.isInstanceOf[CtrlConf]))
  }

  def deployApplication: Unit = deployApplication()

  def deployApplication(hosts: GenSeq[String] = ips.par) {
    syncLib(hosts)
    info("deploying application")
    info("  creating application directories")
    //command(s"mkdir -p ${instDirs.intallationDir}/", hosts, false)
    command(s"mkdir ${instDirs.intallationDir}/app ${instDirs.intallationDir}/conf ${instDirs.intallationDir}/data ${instDirs.intallationDir}/bin", hosts, false)
    command(s"mkdir ${instDirs.intallationDir}/app/batch ${instDirs.intallationDir}/app/bg ${instDirs.intallationDir}/app/ctrl ${instDirs.intallationDir}/app/dc ${instDirs.intallationDir}/app/cas ${instDirs.intallationDir}/app/es ${instDirs.intallationDir}/app/ws ${instDirs.intallationDir}/app/scripts ${instDirs.intallationDir}/app/tools", hosts, false)
    command(s"ln -s ${dataDirs.logsDataDir} ${instDirs.intallationDir}/log", hosts, false)
    info("  deploying components")
    deployComponents(hosts)
    //info("  extracting components")
    //extractComponents(hosts)
    info("  creating symbolic links")


    deployment.componentProps.collect { case cp: DataComponent => cp } foreach {
      _.linkDataDirectories(hosts)
    }
    deployment.componentProps.collect { case cp: LoggingComponent => cp } foreach {
      _.createLoggingDirectories(hosts)
    }
    deployment.componentProps.collect { case cp: ConfigurableComponent => cp } foreach {
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

  private def createAppLinks(hosts: GenSeq[String]) = {

    command(s"test -L ${instDirs.globalLocation}/cm-well/app/batch/logs || ln -s ${instDirs.globalLocation}/cm-well/log/batch/ ${instDirs.globalLocation}/cm-well/app/batch/logs", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/bg/logs || ln -s ${instDirs.globalLocation}/cm-well/log/bg/ ${instDirs.globalLocation}/cm-well/app/bg/logs", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ws/logs || ln -s ${instDirs.globalLocation}/cm-well/log/ws/ ${instDirs.globalLocation}/cm-well/app/ws/logs", hosts, false)
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/log/cw/", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ws/cw-logs || ln -s ${instDirs.globalLocation}/cm-well/log/cw/ ${instDirs.globalLocation}/cm-well/app/ws/cw-logs", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ctrl/logs || ln -s ${instDirs.globalLocation}/cm-well/log/ctrl/ ${instDirs.globalLocation}/cm-well/app/ctrl/logs", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/dc/logs || ln -s ${instDirs.globalLocation}/cm-well/log/dc/ ${instDirs.globalLocation}/cm-well/app/dc/logs", hosts, false)

    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/conf/batch/", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/batch/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/batch/ ${instDirs.globalLocation}/cm-well/app/batch/conf", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/bg/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/bg/ ${instDirs.globalLocation}/cm-well/app/bg/conf", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ws/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/ws/ ${instDirs.globalLocation}/cm-well/app/ws/conf", hosts, false)
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/conf/cw/", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ws/cw-conf || ln -s ${instDirs.globalLocation}/cm-well/conf/cw/ ${instDirs.globalLocation}/cm-well/app/ws/cw-conf", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/ctrl/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/ctrl/ ${instDirs.globalLocation}/cm-well/app/ctrl/conf", hosts, false)
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/conf/dc/", hosts, false)
    command(s"test -L ${instDirs.globalLocation}/cm-well/app/dc/conf || ln -s ${instDirs.globalLocation}/cm-well/conf/dc/ ${instDirs.globalLocation}/cm-well/app/dc/conf", hosts, false)
  }

  def mkScripts(ips: GenSeq[String] = ips): GenSeq[ComponentConf] = {
    null
  }

  def redeploy: Unit = redeploy()

  def redeploy(hosts: GenSeq[String] = ips.par) {
    checkProduction
    stop(false, hosts)
    clearApp(hosts)
    deployApplication(hosts)
  }

  def updateBatch: Unit = updateBatch()

  def updateBatch(hosts: GenSeq[String] = ips) {
    hosts.foreach { h =>
      rsync(s"./components/cmwell-batch_2.10-1.0.1-SNAPSHOT-selfexec.jar", s"${instDirs.globalLocation}/cm-well/app/bg/_cmwell-batch_2.10-1.0.1-SNAPSHOT-selfexec.jar", List(h))
      //stopWebservice(List(hosts))
      stopBatch(List(h))
      command(s"mv ${instDirs.globalLocation}/cm-well/app/bg/_cmwell-batch_2.10-1.0.1-SNAPSHOT-selfexec.jar ${instDirs.globalLocation}/cm-well/app/bg/cmwell-batch_2.10-1.0.1-SNAPSHOT-selfexec.jar", List(h), false)
      startBatch(List(h))
      //startWebservice(List(hosts))
    }

  }

  def updateWebService: Unit = updateWebService()

  def updateWebService(hosts: GenSeq[String] = ips) {
    hosts.foreach { h =>
      command(s"cd ${instDirs.globalLocation}/cm-well/app/ws; mkdir tmp", List(h), false)
      rsync("./components/cmwell-ws_2.10-1.0.1-SNAPSHOT-dist.zip", s"${instDirs.globalLocation}/cm-well/app/ws/tmp/cmwell-ws_2.10-1.0.1-SNAPSHOT-dist.zip", List(h))
      command(s"cd ${instDirs.globalLocation}/cm-well/app/ws/tmp; unzip cmwell-ws_2.10-1.0.1-SNAPSHOT-dist.zip", hosts, false)
      stopWebservice(List(h))
      command(s"rm -rf ${instDirs.intallationDir}/cm-well/app/ws/cmwell-ws-1.0.1-SNAPSHOT", List(h), false)
      command(s"rm ${instDirs.globalLocation}/cm-well/app/ws/RUNNING_PID", List(h), false)
      command(s"mv ${instDirs.globalLocation}/cm-well/app/ws/tmp/cmwell-ws-1.0.1-SNAPSHOT ${instDirs.globalLocation}/cm-well/app/ws/cmwell-ws-1.0.1-SNAPSHOT", List(h), false)
      startWebservice(List(h))
    }
  }

  def removeCmwellSymLink(): Unit = removeCmwellSymLink(ips.par)

  def removeCmwellSymLink(hosts: GenSeq[String]) {
    command(s"unlink ${instDirs.globalLocation}/cm-well 2> /dev/null", hosts, false)
  }

  def createCmwellSymLink(sudoer: Option[Credentials]): Unit = createCmwellSymLink(ips.par, sudoer)

  def createCmwellSymLink(hosts: GenSeq[String], sudoer: Option[Credentials] = None) {
    removeCmwellSymLink(hosts)
    command(s"sudo ln -s ${instDirs.intallationDir} ${instDirs.globalLocation}/cm-well; sudo chown -h $user:$user ${instDirs.globalLocation}/cm-well", hosts, true, sudoer)
  }

  def registerCtrlService(hosts: GenSeq[String], sudoer: Credentials) {
    if (ctrlService) {
      //remove the old ctrl (the link one) - if exists
      command("sudo rm -f /etc/init.d/ctrl", hosts, true, Some(sudoer))
      command(s"mkdir -p ${instDirs.globalLocation}/cm-well/conf/ctrl", hosts, false)
      createFile(s"${instDirs.globalLocation}/cm-well/conf/ctrl/ctrl", Source.fromFile("scripts/templates/ctrl").mkString.replace("{{user}}", user), hosts)
      command(s"chmod +x ${instDirs.globalLocation}/cm-well/conf/ctrl/ctrl", hosts, false)
      val cmwellRunner = Source.fromFile("scripts/templates/cmwell-runner").mkString.replace("\n", "\\\\n") // it's used inside echo -e that will remove the \\ to \ and then another echo -e that will make the actual new line
      createFile("/etc/init.d/cmwell-runner", cmwellRunner, hosts, true, Some(sudoer))
      command("sudo chmod +x /etc/init.d/cmwell-runner", hosts, true, Some(sudoer))
      hosts.foreach {
        host =>
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

  def disks: GenSet[String] = {
    val DataDirs(casDataDirs, casCommitLogDirs, esDataDirs, tlogDataDirs, kafkaDataDir, zookeeperDataDir, logsDataDir) = dataDirs
    val dirs = casDataDirs ++ casCommitLogDirs ++ esDataDirs ++ tlogDataDirs ++ Seq(kafkaDataDir, zookeeperDataDir, logsDataDir, instDirs.intallationDir)
    dirs.map(dir => dir.substring(0, dir.lastIndexOf("/"))).toSet
  }

  def disksWithAncestors(disks: GenSet[String]): GenSet[String] = {
    def addSlash(p: String) = p match {
      case "" => "";
      case "/" => "/";
      case _ => p + "/"
    }

    disks.flatten { disk =>
      val splitted = disk.split("/").map(p => if (p.isEmpty) "/" else p)
      val ancestors = splitted.scan("")((p, dir) => addSlash(p) + dir)
      ancestors.filterNot(p => p.isEmpty || p == "/")
    }
  }

  def unprepareMachines(): Unit = unprepareMachines(ips.par)

  def unprepareMachines(hosts: GenSeq[String]) {
    purge(hosts)
    removeDataDirs(hosts)
    removeCmwellSymLink(hosts)
  }

  def changeOwnerAndAddExcutePermission(hosts: GenSeq[String], dirs: GenSeq[String], user: String, sudoer: Credentials): Unit = {
    dirs.foreach(dir => command(s"sudo chmod +x $dir; sudo chown $user:$user $dir", hosts, true, Some(sudoer)))
  }

  def prepareMachines(): Unit = prepareMachines(ips.par, "", "", "")

  def prepareMachines(hosts: String*): Unit = prepareMachines(hosts, "", "", "")

  def prepareMachines(hosts: GenSeq[String], sudoerName: String, sudoerPass: String, userPass: String) {
    val sudoerNameFinal: String = if (sudoerName != "") sudoerName else scala.io.StdIn.readLine("Please enter sudoer username\n")
    val sudoerPassword: String = if (sudoerPass != "") sudoerPass else scala.io.StdIn.readLine(s"Please enter $sudoerNameFinal password\n")
    println(s"Gaining trust of sudoer account: $sudoerNameFinal")
    gainTrust(sudoerNameFinal, sudoerPassword, hosts)
    sudoerCredentials = Some(Credentials(sudoerNameFinal, sudoerPassword))
    val sudoer = sudoerCredentials.get
    copySshpass(hosts, sudoer)
    println("We will now create a local user 'u' for this cluster")
    val pass = if (userPass != "") userPass else scala.io.StdIn.readLine(s"Please enter $user password\n")
    createUser(user, pass, hosts, sudoer)
    println(s"Gaining trust of the account $user")
    gainTrust(user, pass, hosts)
    refreshUserState(user, Some(sudoer), hosts)
    changeOwnerAndAddExcutePermission(hosts, disksWithAncestors(disks).toSeq, user, sudoer)
    createDataDirs(hosts)
    createCmwellSymLink(hosts, Some(sudoer))
    registerCtrlService(hosts, sudoer)
    finishPrepareMachines(hosts, sudoer)
  }

  protected def finishPrepareMachines(hosts: GenSeq[String], sudoer: Credentials) = {
//    deleteSshpass(hosts, sudoer)
    info("Machine preparation was done. Please look at the console output to see if there were any errors.")
  }

  private def copySshpass(hosts: GenSeq[String], sudoer: Credentials): Unit = {
    //only copy sshpass if it's an internal one
    if (UtilCommands.linuxSshpass == "bin/utils/sshpass") {
      hosts.foreach(host => Seq("rsync", "-z", "-e", "ssh -o StrictHostKeyChecking=no", UtilCommands.linuxSshpass, s"${sudoer.name}@$host:~/bin/") !!)
    }
  }

  private def deleteSshpass(hosts: GenSeq[String], sudoer: Credentials): Unit = {
    command("sudo rm sshpass", hosts, true, Some(sudoer))
  }

  def prepareMachinesNonInteractive: Unit = prepareMachinesNonInteractive()

  def prepareMachinesNonInteractive(sudoerName: String = "mySudoer", sudoerPass: String = "said2000", uPass: String = "said2000", hosts: GenSeq[String] = ips.par) {
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

  def deploy(hosts: GenSeq[String] = ips.par) {
    checkProduction
    deployApplication(hosts)
  }

  def getNewHostInstance(ipms: IpMappings): Host

  def cassandraNetstats = {
    println(command(s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath netstats 2> /dev/null", ips(0), false).get)
  }

  def removeNode(host: String): Host = {
    checkProduction
    connectToGrid
    if (CtrlClient.currentHost == host) CtrlClient.init((ips.toSet - host).head)


    purge(Seq(host))
    Host.ctrl.waitForHealth
    Thread.sleep(20000)
    info("Removing node from Grid")
    Host.ctrl.removeNode(host)

    ipMappings.filePath match {
      case Some(fp) =>
        IpMappingController.writeMapping(ipMappings.remove(List(host)), fp)
        IpMappingController.writeMapping(ipMappings.remove(ips.diff(List(host))), s"${fp}_$host")
      case None => // Do nothing.
    }

    getNewHostInstance(ipMappings.remove(List(host)))
  }

  @deprecated
  def removeNodeOld(host: String): Host = {
    //decommissionCassandraNodes(hosts)
    connectToGrid
    if (CtrlClient.currentHost == host) CtrlClient.init((ips.toSet - host).head)
    val hosts = Seq(host)
    val numberOfCassandraDownNodes = command("ps aux | grep Cassandra | grep -v starter | grep -v grep | wc -l", hosts, false).map(r => r.get.trim.toInt).foldLeft(0)(_ + _)
    //purge(hosts)


    val taskResult = Await.result(CtrlClient.clearNode(host), 1.hours)
    stopCtrl(host)

    val newInstance = getNewHostInstance(ipMappings.remove(hosts.toList))

    //Try(newInstance.CassandraDNLock().waitForModule(newInstance.ips(0), numberOfCassandraDownNodes, 20))
    //newInstance.rebalanceCassandraDownNodes

    info("Regenerating resource files")
    newInstance.genResources()

    if (hosts.contains(ips(0))) {
      val newIndexerMaster = newInstance.ips(0)
      newInstance.stopBatch(List(newIndexerMaster))
      newInstance.startBatch(List(newIndexerMaster))
      info(s"${newInstance.ips(0)}'s indexer will be promoted to be master.")
    }

    ipMappings.filePath match {
      case Some(fp) =>
        IpMappingController.writeMapping(ipMappings.remove(hosts.toList), fp)
        IpMappingController.writeMapping(ipMappings.remove(ips.diff(hosts.toList)), s"${fp}_$host")
      case None => // Do nothing.
    }

    info("Waiting for Health control.")
    Host.ctrl.waitForHealth
    Thread.sleep(20000)
    Host.ctrl.removeNode(host)

    newInstance.dataInitializer.updateKnownHosts
    info(s"The node $host is removed from the cluster.")
    newInstance
  }


  def addNodesSH(path: String) {
    addNodes(path)
    sys.exit(0)
  }

  def removeNodeSH(ip: String) {
    removeNode(ip)
    sys.exit(0)
  }

  def addNodes(path: String): Host = {
    addNodes(IpMappingController.readMapping(path))
  }

  def addNodes(ipms: IpMappings, sudoerName: String = "", sudoerPass: String = "", userPass: String = ""): Host = {
    connectToGrid
    val activeNodes = Try(Await.result(Host.ctrl.getActiveNodes, 10 seconds)).getOrElse(ActiveNodes(Set.empty[String]))
    val addedInstances = getNewHostInstance(ipms)


    //Due to Dudi's request prepare machine isn't run by default and must be run manually (to spare the need for passwords)
    //addedInstances.prepareMachines(addedInstances.ips.par, sudoerName = sudoerName, sudoerPass = sudoerPass, userPass = userPass)
    addedInstances.purge()

    val hostsToRemove = Set.empty[String] //ipMappings.m.map(_.ip).toSet -- activeNodes.an

    val withoutDownNodesMapping = ipMappings.remove(hostsToRemove.toList)
    val combinedMappings = withoutDownNodesMapping combine ipms
    val combinedInstances = getNewHostInstance(combinedMappings)

    combinedInstances.deploy(addedInstances.ips)
    combinedInstances.startCtrl(addedInstances.ips)

    Thread.sleep(20000)

    ipms.getIps.foreach(Host.ctrl.addNode)

    combinedInstances.startDcForced(addedInstances.ips)

    //    combinedInstances.startCassandra(addedInstances.ips)
    //    combinedInstances.startElasticsearch(addedInstances.ips)
    //
    //
    //    Retry{
    //      try{
    //        combinedInstances.CassandraLock().waitForModule(combinedInstances.ips(0), combinedInstances.getSize)
    //      } catch {
    //        case t : Throwable =>
    //          info("Trying to reinit Cassandra")
    //          combinedInstances.startCassandra(addedInstances.ips)
    //          throw t
    //      }
    //    }
    //
    //    Retry{
    //      try{
    //        combinedInstances.ElasticsearchLock().waitForModule(combinedInstances.ips(0), combinedInstances.getSize)
    //      } catch {
    //        case t : Throwable =>
    //          info("Trying to reinit Elasticsearch")
    //          combinedInstances.startElasticsearch(addedInstances.ips)
    //          throw t
    //      }
    //    }
    //
    //    combinedInstances.startCtrl(addedInstances.ips)
    //    combinedInstances.startBatch(addedInstances.ips)
    //    combinedInstances.startWebservice(addedInstances.ips)
    //    combinedInstances.startCW(addedInstances.ips)
    //    combinedInstances.startDc(addedInstances.ips)
    //


    // update the ip mappings file.
    ipMappings.filePath match {
      case Some(fp) => IpMappingController.writeMapping(combinedMappings, fp)
      case None => // Do nothing.
    }
    //combinedInstances.dataInitializer.updateKnownHosts
    combinedInstances
  }

  def killProcess(name: String, flag: String, hosts: GenSeq[String] = ips.par, tries: Int = 5) {
    if (tries > 0) {
      command(s"ps aux | grep -v grep | grep $name | awk '{print $$2}' | xargs -I zzz kill $flag zzz 2> /dev/null", hosts, false)
      val died = command(s"ps aux | grep java | grep -v grep | grep $name | wc -l ", hosts, false).map(s => s.get.trim.toInt).filterNot(_ == 0).length == 0
      if (!died) {
        Thread.sleep(500)
        killProcess(name, flag, hosts, tries - 1)
      }
    } else {
      command(s"ps aux | grep java | grep " + name + " | awk '{print $2}' | xargs -I zzz kill -9 zzz 2> /dev/null", hosts, false)
    }
  }

  // todo: kill with -9 if it didn't work.
  // todo: remove es with its command.
  def stop: Unit = stop(false, ips.par)

  def stop(hosts: String*): Unit = stop(false, hosts.par)

  def stop(force: Boolean, hosts: GenSeq[String]) {
    checkProduction
    val tries = if (force) 0 else 5
    stopWebservice(hosts, tries)
    stopBatch(hosts, tries)
    stopBg(hosts, tries)
    stopElasticsearch(hosts, tries)
    stopCassandra(hosts, tries)
    stopCtrl(hosts, tries)
    stopCW(hosts, tries)
    stopDc(hosts, tries)
    stopKafka(hosts, tries)
    stopZookeeper(hosts, tries)
    stopLogstash(hosts, tries)
    stopKibana(hosts, tries)
  }

  def clearData: Unit = clearData()

  def clearData(hosts: GenSeq[String] = ips.par) {
    checkProduction
    dataDirs.casDataDirs.foreach {
      cas => command(s"rm -rf ${cas}/*", hosts, false)
    }

    dataDirs.casCommitLogDirs.foreach {
      ccl => command(s"rm -rf ${ccl}/*", hosts, false)
    }

    dataDirs.esDataDirs.foreach {
      es => command(s"rm -rf ${es}/*", hosts, false)
    }

    dataDirs.tlogDataDirs.foreach {
      tlog =>
        command(s"rm -rf $tlog/*", hosts, false)
    }

    command(s"rm -rf ${dataDirs.kafkaDataDir}/*; rm -rf ${dataDirs.kafkaDataDir}/.* 2> /dev/null", hosts, false)

    command(s"rm -rf ${dataDirs.zookeeperDataDir}/*", hosts, false)

    command(s"rm -rf ${dataDirs.logsDataDir}/*", hosts, false)
  }

  def clearApp: Unit = clearApp()

  def clearApp(hosts: GenSeq[String] = ips.par) {
    checkProduction
    command(s"rm -rf ${instDirs.intallationDir}/*", hosts, false)
  }

  def purge: Unit = purge()

  def purge(hosts: GenSeq[String] = ips) {
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

  def casHealth(hosts: GenSeq[String] = ips.par): Try[String] = {
    command(s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin" + nodeToolPath + " status", hosts(0), false)
  }

  def esHealth: Try[String] = {
    command("curl -s GET http://" + pingAddress + esHealthAddress, ips(0), false)
  }


  def stopBg: Unit = stopBg(ips.par)

  def stopBg(hosts: String*): Unit = stopBg(hosts.par)

  def stopBg(hosts: GenSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("cmwell.bg.Runner", "", hosts, tries)
  }


  def stopBatch: Unit = stopBatch(ips.par)

  def stopBatch(hosts: String*): Unit = stopBatch(hosts.par)

  def stopBatch(hosts: GenSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("batch", "", hosts, tries)
  }

  def stopWebservice: Unit = stopWebservice(ips.par)

  def stopWebservice(hosts: String*): Unit = stopWebservice(hosts.par)

  def stopWebservice(hosts: GenSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("psId=Webserver", "", hosts, tries)
  }

  def stopCW: Unit = stopCW(ips.par)

  def stopCW(hosts: String*): Unit = stopCW(hosts.par)

  def stopCW(hosts: GenSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("crashableworker", "", hosts, tries)
  }

  def stopDc: Unit = stopDc(ips.par)

  def stopDc(hosts: String*): Unit = stopDc(hosts.par)

  def stopDc(hosts: GenSeq[String], tries: Int = 5) = {
    checkProduction
    killProcess("app/dc", "", hosts, tries)
  }

  def stopCassandra: Unit = stopCassandra(ips.par)

  def stopCassandra(hosts: String*): Unit = stopCassandra(hosts.par)

  def stopCassandra(hosts: GenSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("CassandraDaemon", "", hosts, tries)
  }

  def esSyncedFlush(host: String, port: Int = 9200): Unit = {
    command(s"curl -sX POST 'http://$host:$port/_all/_flush/synced'")
  }

  def stopElasticsearch: Unit = stopElasticsearch(ips.par)

  def stopElasticsearch(hosts: String*): Unit = stopElasticsearch(hosts.par)

  def stopElasticsearch(hosts: GenSeq[String], tries: Int = 5) {
    checkProduction
    esSyncedFlush(hosts(0))
    killProcess("Elasticsearch", "", hosts, tries)
  }


  def startBg: Unit = startBg(ips.par)

  def startBg(hosts: String*): Unit = startBatch(hosts.par)

  def startBg(hosts: GenSeq[String]) {
    checkProduction
    if (withZkKfk)
      command(s"cd ${instDirs.globalLocation}/cm-well/app/bg; ${startScript("./start.sh")}", hosts, false)
  }

  def startBatch: Unit = startBatch(ips.par)

  def startBatch(hosts: String*): Unit = startBatch(hosts.par)

  def startBatch(hosts: GenSeq[String]) {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/batch; ${startScript("./start.sh")}", hosts, false)
  }

  def startWebservice: Unit = startWebservice(ips.par)

  def startWebservice(hosts: String*): Unit = startWebservice(hosts.par)

  def startWebservice(hosts: GenSeq[String]) {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/ws/; ${startScript("./start.sh")}", hosts, false)
  }

  def startCW: Unit = startCW(ips.par)

  def startCW(hosts: String*): Unit = startCW(hosts.par)

  def startCW(hosts: GenSeq[String]) {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/ws; ${startScript("./cw-start.sh")}", hosts, false)
  }

  def startDc: Unit = startDc(ips.par)

  def startDc(hosts: String*): Unit = startDc(hosts.par)

  def startDc(hosts: GenSeq[String]): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/dc; ${startScript("./start.sh")}", hosts, false)
  }

  def startDcForced(hosts: GenSeq[String]): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/dc; HAL=9000 FORCE=MAJOUR ./start.sh", hosts, false)
  }

  //def startScript(script : String) = s"""bash -c "starter '$script'" > /dev/null 2> /dev/null & """
  //def startScript(script : String) = s"""HAL=9000 $script"""
  def startScript(script: String) =
  s"""HAL=9000 ${if (deb) "CMWELL_DEBUG=true" else ""} $script"""

  def start: Unit = start(ips.par)

  def start(hosts: String*): Unit = start(hosts.par)

  def start(hosts: GenSeq[String]) {
    checkProduction
    startCassandra(hosts)
    startElasticsearch(hosts)

    Try(CassandraLock().waitForModule(hosts(0), size))
    Try(ElasticsearchLock().waitForModule(hosts(0), size))
    startZookeeper
    startKafka(hosts)

    startCtrl(hosts)
    startBatch(hosts)
    startCW(hosts)
    startWebservice(hosts)
    startDc(hosts)
    if (withElk) {
      startLogstash(hosts)
      startKibana(hosts)
    }
  }

  def startCtrl: Unit = startCtrl(ips)

  def startCtrl(hosts: String*): Unit = startCtrl(hosts.par)

  def startCtrl(hosts: GenSeq[String]) = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/ctrl; ${startScript("./start.sh")}", hosts, false)
  }

  def stopCtrl: Unit = stopCtrl(ips.par)

  def stopCtrl(hosts: String*): Unit = stopCtrl(hosts.par)

  def stopCtrl(hosts: GenSeq[String], tries: Int = 5) {
    checkProduction
    killProcess("CtrlServer", "", hosts, tries)
  }

  def createManager: Unit = createManager()

  def createManager(machineName: String = ips(0), path: String = "~/cmwell/") {
    rsync("./", path, List(machineName))
  }

  def readTime(targ: String = "meta/ns/oa") {

  }


  def init: Unit = init()

  def init(hosts: GenSeq[String] = ips.par) {
    checkProduction
    info("starting controller")
    startCtrl(hosts)
    info("initializing cm-well")
    info("  initializing cassandra")
    initCassandra(hosts)
    info("  initializing elasticsearch")
    initElasticsearch(hosts)
    info("  waiting for cassandra and elasticsearch")

    Retry {
      try {
        CassandraLock().waitForModule(hosts(0), size)
      } catch {
        case t: Throwable =>
          info("Trying to reinit Cassandra")
          initCassandra(hosts)
          throw t
      }
    }

    Retry {
      try {
        ElasticsearchLock().waitForModule(hosts(0), size)
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
    initSchemes(hosts)
    // wait until all the schemas are written.
    Thread.sleep(10000)

    info("  starting batch")
    startBatch(hosts)
    info("  starting bg")
    startBg(hosts)
    info(" starting cw")
    startCW(hosts)
    info("  starting web service")
    startWebservice(hosts)
    uploadInitialContent(hosts(0))
    info("  starting dc controller")
    startDc(hosts)

    info("finished initializing cm-well")
    if (withElk) {
      startLogstash(hosts)
      startKibana(hosts)
    }
  }


  def uploadInitialContent(host: String = ips(0)): Unit = {
    checkProduction
    Try(WebServiceLock().waitForModule(host, 1))

    info("  waiting for ws...")
    dataInitializer.waitForWs()
    info("  inserting meta data")
    injectMetaData(host)
    info("  uploading SPA to meta/app")
    dataInitializer.uploadDirectory("data", s"http://$host:9000/meta/app/")
    info("  uploading sys")
    dataInitializer.uploadDirectory("sys", s"http://$host:9000/meta/sys/wb/")
    info("  uploading docs")
    dataInitializer.uploadDirectory("docs", s"http://$host:9000/meta/docs/")
    info("  uploading basic userInfotons (if not exist)")
    dataInitializer.uploadBasicUserInfotons(host)
    info("  updating version history")
    dataInitializer.logVersionUpgrade(host)
  }

  def initCassandra: Unit = initCassandra()

  def initCassandra(hosts: GenSeq[String] = ips.par)

  def initElasticsearch: Unit = initElasticsearch()

  def initElasticsearch(hosts: GenSeq[String] = ips.par)

  def initSchemes: Unit = initSchemes()

  def initSchemes(hosts: GenSeq[String] = ips.par) {
    val aliases =
      """{
                     "actions" : [
                            { "add" : { "index" : "cmwell_current_0", "alias" : "cmwell_current" } },
                            { "add" : { "index" : "cmwell_history_0", "alias" : "cmwell_history" } },
                            { "add" : { "index" : "cmwell_current_0", "alias" : "cmwell_current_latest" } },
                            { "add" : { "index" : "cmwell_history_0", "alias" : "cmwell_history_latest" } },
                            { "add" : { "index" : "cm_well_p0_0", "alias" : "cm_well_all" } }
                        ]
                    }""".replace("\n", "")
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur; sh bin/cqlsh ${pingAddress} -f ${instDirs.globalLocation}/cm-well/conf/cas/cassandra-cql-init-cluster", hosts(0), false)
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur; sh bin/cqlsh ${pingAddress} -f ${instDirs.globalLocation}/cm-well/conf/cas/cassandra-cql-init-cluster-new", hosts(0), false)
    command(s"cd ${instDirs.globalLocation}/cm-well/app/cas/cur; sh bin/cqlsh ${pingAddress} -f ${instDirs.globalLocation}/cm-well/conf/cas/zstore-cql-init-cluster", hosts(0), false)
    command(s"""curl -s -X POST http://${pingAddress}:$esRegPort/_template/cmwell_indices_template -H "Content-Type: application/json" --data-ascii @${instDirs.globalLocation}/cm-well/conf/es/mapping.json""", hosts(0), false)
    command(s"""curl -s -X POST http://${pingAddress}:$esRegPort/_template/cmwell_index_template -H "Content-Type: application/json" --data-ascii @${instDirs.globalLocation}/cm-well/conf/es/indices_template_new.json""", hosts(0), false)
    command(s"curl -s -X POST http://${pingAddress}:$esRegPort/cmwell_current_0/;curl -s -X POST http://${pingAddress}:$esRegPort/cmwell_history_0/", hosts(0), false)
    command(s"curl -s -X POST http://${pingAddress}:$esRegPort/cm_well_p0_0/", hosts(0), false)
    //    command(s"curl -s -X POST http://${pingAddress}:$esRegPort/cm_well_0/", hosts(0), false)
    command(s"""curl -s -X POST http://${pingAddress}:$esRegPort/_aliases -H "Content-Type: application/json" --data-ascii '${aliases}'""", hosts(0), false)
    // create kafka topics
    if (withZkKfk) {
      val replicationFactor = math.min(hosts.size, 3)
      val createTopicCommandPrefix = s"cd ${instDirs.globalLocation}/cm-well/app/kafka/cur; export PATH=/opt/cm-well/app/java/bin:$$PATH ; sh bin/kafka-topics.sh --create --zookeeper ${pingAddress}:2181 --replication-factor $replicationFactor --partitions ${hosts.size} --topic"
      var tryNum:Int = 1
      var ret = command(s"$createTopicCommandPrefix persist_topic", hosts(0), false)
      while(ret.isFailure || !ret.get.contains("Created topic") && tryNum < 6 ){
        tryNum += 1
        Thread.sleep(5000)
        ret = command(s"$createTopicCommandPrefix persist_topic", hosts(0), false)
      }

      ret = command(s"$createTopicCommandPrefix index_topic", hosts(0), false)
      while(ret.isFailure || !ret.get.contains("Created topic") && tryNum < 6 ){
        tryNum += 1
        Thread.sleep(5000)
        ret = command(s"$createTopicCommandPrefix index_topic", hosts(0), false)
      }

    }
  }

  val withZookeeper = withZkKfk
  val withKafka = withZkKfk

  def avaiableHosts = {
    ips.filter {
      ip =>
        command(s"ping -c 1 $ip > /dev/null 2> /dev/null").isSuccess
    }

  }

  def brokerId(host: String) = ips.indexOf(host)


  def startZookeeper: Unit = {
    checkProduction
    if (withZookeeper) command(s"cd ${instDirs.globalLocation}/cm-well/app/zookeeper; ${startScript("./start.sh")}", avaiableHosts.take(3), false)
  }


  def startZookeeper(host: String): Unit = {
    if (withZookeeper) command(s"cd ${instDirs.globalLocation}/cm-well/app/zookeeper; ${startScript("./start.sh")}", host, false)
  }

  def startZookeeper(hosts: GenSeq[String]): Unit = {
    checkProduction
    if (withZookeeper)
      command(s"cd ${instDirs.globalLocation}/cm-well/app/zookeeper; ${startScript("./start.sh")}", hosts.intersect(avaiableHosts), false)
  }

  def stopZookeeper: Unit = stopZookeeper()

  def stopZookeeper(hosts: GenSeq[String] = ips.par, tries: Int = 5): Unit = {
    checkProduction
    //if(withZookeeper)
    killProcess("zookeeper", "", hosts, tries = tries)
  }


  def startKafka: Unit = startKafka()

  def startKafka(hosts: GenSeq[String] = ips.par): Unit = {
    checkProduction
    if (withKafka) {
      command(s"cd ${instDirs.globalLocation}/cm-well/app/kafka; ${startScript("./start.sh")}", hosts, false)
    }
  }

  def startKafka(host: String): Unit = {
    checkProduction
    if (withKafka) {
      command(s"cd ${instDirs.globalLocation}/cm-well/app/kafka; ${startScript("./start.sh")}", host, false)
    }
  }

  def stopKafka: Unit = stopKafka()

  def stopKafka(hosts: GenSeq[String] = ips.par, tries: Int = 5): Unit = {
    checkProduction
    //if(withKafka)
    killProcess("kafka.Kafka", "", hosts, tries = tries)
  }

  def startElasticsearch: Unit = startElasticsearch(ips.par)

  def startElasticsearch(hosts: String*): Unit = startElasticsearch(hosts.par)

  def startElasticsearch(hosts: GenSeq[String]): Unit

  def startCassandra: Unit = startCassandra(ips.par)

  def startCassandra(hosts: String*): Unit = startCassandra(hosts.par)

  def startCassandra(hosts: GenSeq[String])


  def startKibana: Unit = startKibana()

  def startKibana(hosts: GenSeq[String] = ips.par): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/kibana; ${startScript("./start.sh")}", hosts, false)
  }

  def stopKibana: Unit = stopKibana()

  def stopKibana(hosts: GenSeq[String] = ips.par, tries: Int = 5): Unit = {
    checkProduction
    killProcess("kibana", "", hosts, tries = tries)
  }

  def startLogstash: Unit = startLogstash()

  def startLogstash(hosts: GenSeq[String] = ips.par): Unit = {
    checkProduction
    command(s"cd ${instDirs.globalLocation}/cm-well/app/logstash; ${startScript("./start.sh")}", hosts, false)
  }

  def stopLogstash: Unit = stopLogstash()

  def stopLogstash(hosts: GenSeq[String] = ips.par, tries: Int = 5): Unit = {
    checkProduction
    killProcess("logstash", "", hosts, tries = tries)
  }

  def quickInstall: Unit = {
    checkProduction
  }

  def install: Unit = install(ips.par)

  def install(hosts: String*): Unit = install(hosts.par)

  def install(hosts: GenSeq[String]) {
    checkProduction
    refreshUserState(user, None, hosts)
    purge(hosts)
    deploy(hosts)
    init(hosts)
    //setElasticsearchUnassignedTimeout()
  }


  def disableElasticsearchUpdate: Unit = disableElasticsearchUpdate(ips(0))

  def disableElasticsearchUpdate(ip: String) {
    command(s"""curl -s -X PUT http://${pingAddress}:$esRegPort/_cluster/settings -d '{"transient" : {"cluster.routing.allocation.enable" : "none"}}'""", ip, false)
  }


  def enableElasticsearchUpdate: Unit = enableElasticsearchUpdate(ips(0))

  def enableElasticsearchUpdate(ip: String) {
    command(s"""curl -s -X PUT http://${pingAddress}:$esRegPort/_cluster/settings -d '{"transient" : {"cluster.routing.allocation.enable" : "all"}}'""", ip, false)
  }

  def findEsMasterNode(hosts: GenSeq[String] = ips): Option[String] = {
    hosts.par.find(host => command(s"curl -s $host:$esMasterPort > /dev/null 2> /dev/null").isSuccess)
  }

  def findEsMasterNodes(hosts: GenSeq[String] = ips): GenSeq[String] = {
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
    command(s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath status 2> /dev/null | grep $addr | awk '{print $$7}'", ips(0), false).get.trim
  }


  def rebalanceCassandraDownNodes {
    // grep DN | awk '{print $2 " " $7}'
    Retry {
      val downNodes = command(s"""JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath status 2> /dev/null | grep DN | awk '{print $$2 " " $$7}'""", ips(0), false).get.trim.split("\n").toList.map {
        dn =>
          val dnsplt = dn.split(" ")
          dnsplt(0) -> dnsplt(1)
      }

      downNodes.par.foreach(dn => command(s"JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath removenode ${dn._2} 2> /dev/null", ips(0), false))
      if (command( s"""JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolPath status 2> /dev/null | grep DN | awk '{print $$2 " " $$7}'""", ips(0), false).get.trim.split("\n").toList.size > 0)
        throw new Exception("Failed to remove down nodes")

      info(s"Cassandra nodes were removed from the cluster. The cluster now will rebalance its data.")
    }
  }

  def getCassandraAddresses(host: String): Seq[String] = Seq(host)

  def decommissionCassandraNodes(hosts: GenSeq[String]) {
    hosts.foreach {
      host =>
        getCassandraAddresses(host).foreach {
          ip =>
            command( s"""JAVA_HOME=${instDirs.globalLocation}/cm-well/app/java/bin $nodeToolLocation -h $ip decommission 2> /dev/null""", host, false)
        }

    }
  }


  def shutdown: Unit = shutdown()

  def shutdown(hosts: GenSeq[String] = ips): Unit = {
    disableElasticsearchUpdate
    stop(false, hosts)
  }


  def updateCasKeyspace: Unit = {
    command(s"cd ${absPath(instDirs.globalLocation)}/cm-well/app/cas/cur; sh bin/cqlsh ${pingAddress} -f ${absPath(instDirs.globalLocation)}/cm-well/conf/cas/cassandra-cql-init-cluster-new", ips(0), false)
  }

  def updateKafkaScemas: Unit = {
    val replicationFactor = math.min(ips.size, 3)
    val createTopicCommandPrefix = s"cd ${absPath(instDirs.globalLocation)}/cm-well/app/kafka/cur; export PATH=/opt/cm-well/app/java/bin:$$PATH ; sh bin/kafka-topics.sh --create --zookeeper ${pingAddress}:2181 --replication-factor $replicationFactor --partitions ${ips.size} --topic"
    command(s"$createTopicCommandPrefix persist_topic", ips(0), false)
    command(s"$createTopicCommandPrefix index_topic", ips(0), false)
  }


  def checkPreUpgradeStatus(host: String): Unit = {
    val esStatusTry = elasticsearchStatus(host)
    val casStatusTry = cassandraStatus(host).map(_.toInt)
    val wsStatusTry = webServerStatus(host)

    var hasProblem = false

    esStatusTry match {
      case Success(color) => if (color.toLowerCase != "green") {
        hasProblem = true
        warn(s"Elasticsearch status is $color.")
      }
      case Failure(err) =>
        hasProblem = true
        warn(s"Couldn't retrieve Elasticsearch status.")
    }

    casStatusTry match {
      case Success(uns) => if (uns < size) {
        hasProblem = true
        warn(s"Number of Cassandra up nodes is $uns/$size.")
      }
      case Failure(err) =>
        hasProblem = true
        warn(s"Couldn't retrieve Cassandra status.")
    }

    wsStatusTry match {
      case Success(v) => if (!v.contains("200") && !v.contains("404") && !v.contains("503")) {
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

  def upgradeBG = upgrade(List(BatchProps(this)), uploadSpa = false, uploadDocs = false)

  def upgradeWS = upgrade(List(WebserviceProps(this)))

  def quickUpgrade: Unit = quickUpgrade()

  def quickUpgrade(hosts: GenSeq[String] = ips): Unit = {
    refreshUserState(user, None, hosts)
    syncLib(hosts)
    linkLibs(hosts)
    hosts.par.foreach(host => restartApp(host))
  }


  def noDownTimeQuickUpgrade(hosts: GenSeq[String] = ips): Unit = {
    refreshUserState(user, None, hosts)
    info("syncing libs")
    syncLib(hosts)
    linkLibs(hosts)

    info("generating resources")
    genResources(hosts)


    info("stopping CM-WELL components")
    stopBatch(hosts)
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
    hosts1.foreach { host => info(s"waiting for $host to respond"); WebServiceLock().com(host) }

    info(s"restarting web services on ${hosts2.mkString(",")}")
    stopWebservice(hosts2)
    startWebservice(hosts2)

    hosts2.foreach { host => info(s"waiting for $host to respond"); WebServiceLock().com(host) }

    startBatch(hosts)
    startBg(hosts)
    startDc(hosts)
    startCW(hosts)
    startCtrl(hosts)
  }

  def upgrade: Unit = upgrade()

  def upgrade(baseProps: List[ComponentProps] = List(CassandraProps(this), ElasticsearchProps(this), KafkaProps(this), ZooKeeperProps(this), BgProps(this), BatchProps(this), WebserviceProps(this), CtrlProps(this), DcProps(this), TlogProps(this)), clearTlogs: Boolean = false, uploadSpa: Boolean = true, uploadDocs: Boolean = true, uploadUserInfotons: Boolean = true, withUpdateSchemas: Boolean = false, hosts: GenSeq[String] = ips) {

    checkProduction
    refreshUserState(user, None, hosts)

    //checkPreUpgradeStatus(hosts(0))
    val esMasterNode = findEsMasterNode(hosts) match {
      case Some(emn) =>
        info(s"found Elasticsearch master node: $emn")
        emn
      case None => throw new Exception("Couldn't find elasticsearch master node")

    }
    val dateStr = deployment.getCurrentDateStr
    var props = baseProps

    if (deployJava) props = props ++ List(JavaProps(this))
    if (withElk) props = props ++ List(LogstashProps(this), KibanaProps(this))


    info("deploying components and checking what should be upgraded.")
    syncLib(hosts)
    linkLibs(hosts)
    rsyncPlugins(hosts)
    BinsProps(this).deployComponent(hosts)

    //println(s"props: $props")

    // get for each component its unsynced hosts and redeploy the new version of the component.
    val updatedHosts = props.map(prop => (prop, prop.getUnsyncedHosts(hosts.par))).filter(t => t._2.size > 0).map(t => (t._1, t._2, t._1.redeployComponent(t._2)))
    if (updatedHosts.size > 0) {
      //todo: FIX THIS!!!
      doInfo = false
      deployment.createDirs(hosts, props)
      doInfo = true
      val updatedComponents = updatedHosts.map(_._1).toSet
      val preUpgradeComponents = props.collect { case r: RunnableComponent if r.upgradeMethod == PreUpgrade => r }.filter(r => updatedComponents.contains(r) || !updatedComponents.intersect(r.upgradeDependency).isEmpty)
      val nonRollingComponents = props.collect { case r: RunnableComponent if r.upgradeMethod == NonRolling => r }.filter(r => updatedComponents.contains(r) || !updatedComponents.intersect(r.upgradeDependency).isEmpty)
      val rollingComponents = props.collect { case r: RunnableComponent if r.upgradeMethod == Rolling => r }.filter(r => updatedComponents.contains(r) || !updatedComponents.intersect(r.upgradeDependency).isEmpty)
      val nonRunningComponents = props.filter(p => !p.isInstanceOf[RunnableComponent])

      updatedHosts.filter { el => nonRunningComponents.contains(el._1) && el._1.symLinkName.isDefined }.foreach {
        el =>
          val component = el._1
          val hostsToUpdate = el._2
          val newName = el._3
          info(s"updating ${component.getName} on all hosts")
          component.relink(newName, hostsToUpdate)
      }


      // stopping all the components that are not upgraded in rolling style.
      nonRollingComponents.foreach {
        nrc =>
          info(s"stopping ${nrc.getName} on all hosts.")
          nrc.stop(hosts)
      }


      hosts.foreach {
        h =>
          // The components that where updated on this host.
          val updatedHostComponents = updatedHosts.filter(uh => uh._2.toVector.contains(h)).map(uh => uh._1 -> (uh._2, uh._3)).toMap
          val casUpdated = updatedComponents.contains(CassandraProps(this))
          val esUpdated = updatedComponents.contains(ElasticsearchProps(this))
          val javaUpdated = updatedComponents.contains(JavaProps(this))

          //if(esUpdated || javaUpdated) {
          Try(ElasticsearchLock().waitForModule(esMasterNode, size))
          Try(ElasticsearchStatusLock("green", "yellow").waitForModuleIndefinitely(esMasterNode))
          // if we encounter status yellow lets sleep for 10 minutes.
          //if(elasticsearchStatus(ips(0)).getOrElse("N/A") == "yellow") Thread.sleep(10 * 1000 * 60)
          //}

          info(s"updating ${(updatedComponents -- nonRunningComponents -- preUpgradeComponents).map(_.getName).mkString(", ")} on $h")

          val updatedComponentsSet = updatedComponents
          // stopping all the components that are upgraded in rolling style.
          rollingComponents.foreach {
            rc =>
              info(s"  restarting ${rc.getName}")
              rc.stop(List(h))
          }

          if (clearTlogs) {
            removeTlogs(List(h))
          }

          // relinking the new components.
          (updatedComponentsSet -- preUpgradeComponents -- nonRunningComponents).foreach(cp => if (cp.symLinkName.isDefined) cp.relink(updatedHostComponents.get(cp).get._2, List(h)))

          createAppLinks(List(h))
          genResources(List(h))

          // starting all the components that are upgraded in rolling style.
          rollingComponents.foreach(_.start(List(h)))

          // wait for cassandra and elasticsearch to be stable before starting cmwell components.
          if (javaUpdated || casUpdated) {
            Try(CassandraLock().waitForModule(ips(0), size))
          }


      }


      hosts.par.foreach(host => Try(WebServiceLock().waitForModule(host, 1)))

      preUpgradeComponents.foreach {
        puc =>
          info(s"restarting ${puc.getName} on all hosts")
          puc.stop(hosts)
      }
      updatedHosts.filter { el => preUpgradeComponents.contains(el._1) && el._1.symLinkName.isDefined }.foreach {
        el =>
          val component = el._1
          val hostsToUpdate = el._2
          val newName = el._3
          info(s"updating ${component.getName} on all hosts.")
          component.relink(newName, hostsToUpdate)
      }

      // todo: make more generic.
      genEsResources(hosts)
      preUpgradeComponents.foreach(_.start(hosts))

      // starting all the components that are not upgraded in rolling style.

      Try(ElasticsearchLock(esMasterNode).waitForModule(esMasterNode, size))
      Try(ElasticsearchStatusLock("green", "yellow").waitForModuleIndefinitely(esMasterNode))


      if (withUpdateSchemas) {
        updateCasKeyspace
        reloadEsMappings
        updateKafkaScemas
      }

      nonRollingComponents.par.foreach {
        nrc =>
          info(s"starting ${nrc.getName} on all hosts.")
          nrc.start(hosts)
      }
    }

    Try(WebServiceLock().waitForModule(ips(0), 1))
    info("  waiting for ws...")
    dataInitializer.waitForWs()

    if (uploadSpa) {
      Try(WebServiceLock().waitForModule(ips(0), 1))
      info("  uploading SPA to meta/app")
      dataInitializer.uploadDirectory("data", s"http://${hosts.head}:9000/meta/app/")
      info("  uploading sys")
      dataInitializer.uploadDirectory("sys", s"http://${hosts.head}:9000/meta/sys/wb/")
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
  }


  def reloadEsMappings: Unit = reloadEsMappings()

  def reloadEsMappings(createNewIndices: Boolean = true) {

    info("reloading Elasticsearch mappings")
    command(s"""curl -s -X POST http://${pingAddress}:$esRegPort/_template/cmwell_index_template -H "Content-Type: application/json" --data-ascii @${absPath(instDirs.globalLocation)}/cm-well/conf/es/indices_template_new.json""", ips(0), false)

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
    val numberOfShards = getSize
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

    val (currents, histories) = JSON.parseFull(json.trim).get.asInstanceOf[Map[String, Any]]("indices").asInstanceOf[Map[String, Any]].keySet.partition {
      _.contains("current")
    }


    val currentIndex = currents.map(_.split("_")(2).toInt).max
    val historyIndex = histories.map(_.split("_")(2).toInt).max


    val newCurrentIndex = s"cmwell_current_${currentIndex + 1}"
    val newHistoryIndex = s"cmwell_history_${historyIndex + 1}"

    val oldCurrentIndex = s"cmwell_current_$currentIndex"
    val oldHistoryIndex = s"cmwell_history_$historyIndex"


    command(s"""curl -s -XPUT 'http://${pingAddress}:$esRegPort/$newCurrentIndex/' -d '$settingsJson'""", ips.head, false)
    command(s"""curl -s -XPUT 'http://${pingAddress}:$esRegPort/$newHistoryIndex/' -d '$settingsJson'""", ips.head, false)


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

    stopBatch
    startBatch
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

    stopBatch(host)
    startBatch(host)
  }

  def restartWebservice {
    ips.foreach {
      ip =>
        info(s"Restarting Webservice on $ip")
        stopWebservice(Seq(ip))
        startWebservice(Seq(ip))
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
    ips.foreach {
      ip =>
        info(s"Restarting Cassandra on $ip")
        stopCassandra(Seq(ip))
        startCassandra(Seq(ip))
        Try(CassandraLock().waitForModule(ips(0), size))
    }
  }

  def restartElasticsearch: Unit = restartElasticsearch(ips)

  def restartElasticsearch(hosts: Seq[String]) {
    hosts.foreach {
      host =>
        Try(ElasticsearchStatusLock("green").waitForModule(hosts(0), 1000))
        info(s"Restarting Elasticsearch on $host")
        disableElasticsearchUpdate(ips((ips.indexOf(host) + 1) % ips.size))
        Thread.sleep(10000)
        stopElasticsearch(Seq(host))
        startElasticsearch(Seq(host))
        enableElasticsearchUpdate(ips((ips.indexOf(host) + 1) % ips.size))
    }
  }

  //def createNetwork : Unit = createNetwork(ips.par,topology, persistentAliases)
  def createNetwork(topology: NetTopology, persistent: Boolean, sudoer: Credentials) {
    topology match {
      case n: VLanTopology =>
        val tag = n.tag
        val m = topology.getTopologyMap(ipMappings)
        m.foreach {
          tuple =>
            var index = 0
            command(s"echo '/sbin/modprobe 8021q' | sudo tee /etc/sysconfig/modules/vlan.modules > /dev/null", tuple._1, true, Some(sudoer))
            command(s"sudo chmod +x /etc/sysconfig/modules/vlan.modules", tuple._1, true, Some(sudoer))
            command(s"sudo modprobe 8021q", tuple._1, true, Some(sudoer))

            command(s"sudo ip link add link $inet name $inet.$tag type vlan id $tag", tuple._1, true, Some(sudoer))
            command(s"sudo ifconfig $inet.$tag up", tuple._1, true, Some(sudoer))

            val fileName = s"ifcfg-$inet.$tag"
            val path = "/etc/sysconfig/network-scripts"
            val fileContent =
              s"""
                 |DEVICE=$inet.$tag
                 |BOOTPROTO=none
                 |ONBOOT=yes
                 |VLAN=yes
              """.stripMargin
            command(s"echo '$fileContent' | sudo tee $path/$fileName > /dev/null", tuple._1, true, Some(sudoer))
            tuple._2.foreach {
              ip =>
                val mask = topology.getNetMask
                val fileName = s"ifcfg-$inet.$tag:$index"
                val path = "/etc/sysconfig/network-scripts"
                val fileContent =
                  s"""
                     |DEVICE=${inet}.${tag}:${index}
                     |IPADDR=${ip}
                     |NETMASK=$mask
                     |ONBOOT=yes
                  """.stripMargin
                command(s"echo '$fileContent' | sudo tee $path/$fileName > /dev/null", tuple._1, true, Some(sudoer))
                command(s"sudo ifconfig $inet.$tag:$index $ip netmask $mask", tuple._1, true, Some(sudoer))
                index += 1
            }
        }
      case _ =>
        val m = topology.getTopologyMap(ipMappings)
        m.foreach {
          tuple =>
            var index = 0
            tuple._2.foreach {
              ip =>
                command(s"sudo ifconfig $inet:$index $ip/${topology.getCidr} up", tuple._1, true, Some(sudoer))
                if (persistent) {
                  val path = "/etc/sysconfig/network-scripts"
                  val fileName = s"ifcfg-$inet:$index"
                  val fileContent =
                    s"""
                       |DEVICE=$inet:$index
                       |IPADDR=$ip
                       |NETMASK=${topology.getNetMask}
                       |ONBOOT=yes
                 """.stripMargin
                  command(s"echo '$fileContent' | sudo tee $path/$fileName > /dev/null", tuple._1, true, Some(sudoer))
                }
                index += 1
            }
        }
    }
  }

  def removeTlogs(ips: GenSeq[String] = ips.par) {
    command(s"rm ${dataDirs.tlogDataDirs(0)}/*", ips, false)
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
      val validInterfaceOpt = interfaces.collectFirst { case i if (command(s"ping -c 1 -I ${i.getName} $ipToCheckAgainst ; echo $$?").get.split("\n").toList.last.trim == "0") => i}
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
    ips.foreach {
      ip =>
        val res = getIpInGrid(ip)
        if (res.isDefined) return res.get
    }
    ips(0)
  }

  def getIpInGrid(ipToCheckAgainst: String): Option[String] = {
    import java.net.NetworkInterface
    import java.util

    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._

    val interfaces: Seq[java.net.NetworkInterface] = util.Collections.list(NetworkInterface.getNetworkInterfaces())
    val validInterfaceOpt = interfaces.collectFirst { case i if (command(s"ping -c 1 -I ${i.getName} $ipToCheckAgainst ; echo $$?").get.split("\n").toList.last.trim == "0") => i }
    validInterfaceOpt match {
      case Some(validInterface) =>
        validInterface.getInterfaceAddresses.asScala.collectFirst {
          case inetAddr if (inetAddr.getAddress.getHostAddress.matches( """\d+.\d+.\d+.\d+""")) =>
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
      AkkaGrid.setGridConnection(GridConnection(memberName = "CONS", clusterName = cn, hostName = useIp, port = 0, seeds = ips.take(3).map(seedIp => s"$seedIp:7777").toSet))
      AkkaGrid.joinClient

      CtrlClient.init(ips(0))
      Host.connectedToAkkaGrid = true
      Thread.sleep(5000)
    }
  }


  def restartHaproxy(sudoer: Credentials) {
    haProxy match {
      case Some(HaProxy(host, sitedown)) =>
        command("sudo service haproxy restart", Seq(host), true, Some(sudoer))
      case None =>
    }
  }

  def stopHaproxy(sudoer: Credentials) {
    haProxy match {
      case Some(HaProxy(host, sitedown)) =>
        command("sudo service haproxy stop", Seq(host), true, Some(sudoer))
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

  private val elkImageName = "cmwell-elk"
  private val elkContainerName = "cmwell-elk-container"
  private val elkClusterNameSuffix = "elk"
  private val elkDirName = "elk"
  private val elkEsWebPort = 9220
  private val elkEsTransportPort = 9320
  private val elkWebPort = 8080

  def deployElk: Unit = {
    ???
    info(s"copying files to remote hosts.")
    ips.par.foreach {
      ip =>
        info(s"copying files to $ip")
        command(s"rsync -Paz scripts/docker-elk $user@$ip:${instDirs.intallationDir}/app/")
    }

    info(s"creating docker image")
    ips.par.foreach {
      ip =>
        val res = command(s"sudo cd ${instDirs.intallationDir}/app/docker-elk/; sudo docker build -t $elkImageName .", ip, true)
        if (res.isSuccess)
          info(s"image was created at $ip")
        else
          info(s"failed to create image at $ip")
    }

    info("creating elk log directory")
    command(s"mkdir -p ${instDirs.intallationDir}/log/$elkDirName", ips, false)
  }


  def createLogstashConfig: Unit = {
    info("creating logstash config file")
    ips.par.foreach {
      ip =>
        createLogstashConfFile(s"$ip:$elkEsWebPort", Seq(ip))
    }
  }

  def startElk: Unit = {
    def getSeeds: String = {
      ips.take(3).map(ip => s"$ip:$elkEsTransportPort").mkString(",")
    }

    //docker run -e elk_cluster='docker-elk' -v /home/michael/me/projects/elk-docker/conf:/etc/logstash -v /home/michael/app/cm-well/log:/cm-well/log  -p 8080:80 -p 9200:9220 -p 9300:9320 elk
    //command(s"docker run -d --net=host --name=$elkContainerName -e elk_cluster='$cn-$elkClusterNameSuffix' -e elk_hosts='$getSeeds' -v ${instDirs.intallationDir}/conf/logstash/:/etc/logstash -v ${instDirs.intallationDir}/log:/opt/cm-well/log -v ${instDirs.intallationDir}/log/$elkDirName:/usr/share/elasticsearch/data -p $elkWebPort:80 -p $elkEsWebPort:$elkEsWebPort -p $elkEsTransportPort:$elkEsTransportPort $elkImageName", ips, true)
    ???
  }

  def stopElk: Unit = {
    //command(s"docker rm -f $elkContainerName", ips, true)
    ???
  }


  def removeOldPackages(hosts: GenSeq[String] = ips): Unit = {
    val packs = deployment.componentProps.filter(_.symLinkName.isDefined)
    val loc = instDirs.globalLocation

    for {
      host <- hosts
      pack <- packs
    } {
      val target = pack.targetLocation
      val compName = pack.getName
      val symLinkName = pack.symLinkName.get
      val currentPack = command(s"readlink -e $loc/cm-well/$target/$symLinkName | xargs basename", host, false).get.trim
      val com = s"ls -1 $loc/cm-well/$target | grep $compName | grep -v $currentPack | xargs -I zzz rm -rf $loc/cm-well/$target/zzz"
      command(com, host, false)
    }
  }

  def syncLib(hosts: GenSeq[String] = ips) = {
    def getCurrentDateStr = {
      val format = new java.text.SimpleDateFormat("yyyyMMdd_hhmmss")
      val date = new Date()
      format.format(date)
    }

    val currentDate = getCurrentDateStr

    hosts.foreach {
      host =>
        val comStr =
          s"""test -L ${instDirs.globalLocation}/cm-well/lib &&
             |cp -al `readlink ${instDirs.globalLocation}/cm-well/lib`/ ${instDirs.globalLocation}/cm-well/lib-$currentDate/ ||
             |mkdir -p ${instDirs.globalLocation}/cm-well/lib-$currentDate""".stripMargin

        command(comStr, host, false)

        command(s"test -L ${instDirs.globalLocation}/cm-well/lib && rm ${instDirs.globalLocation}/cm-well/lib", host, false)
        command(s"ln -s ${instDirs.globalLocation}/cm-well/lib-$currentDate ${instDirs.globalLocation}/cm-well/lib", host, false)

        rsync("lib/", s"${instDirs.globalLocation}/cm-well/lib/", Seq(host))
    }

  }


  def linkLibs(hosts: GenSeq[String] = ips.par) = {
    val dir = new File("dependencies")

    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/dependencies", hosts, false)
    rsync(s"dependencies/", s"${instDirs.globalLocation}/cm-well/dependencies/", hosts)

    dir.listFiles().toVector.par.foreach {
      file =>
        linkLib(file.getName, hosts)
    }
  }

  def linkLib(component: String, hosts: GenSeq[String] = ips) = {

    val target = component //if(component == "batch") "bg" else component

    //val content = Source.fromFile(s"dependencies/$component").getLines().toVector


    command(s"rm ${instDirs.globalLocation}/cm-well/app/$target/lib/* > /dev/null 2> /dev/null", hosts, false)
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/app/$target/lib", hosts, false)

    hosts.foreach {
      host =>
        command(s"cat ${instDirs.globalLocation}/cm-well/dependencies/$component | xargs -I DEP ln -s ${instDirs.globalLocation}/cm-well/lib/DEP ${instDirs.globalLocation}/cm-well/app/$target/lib/DEP", host, false)
    }
  }

  sys.addShutdownHook {
    Try(k.grid.Grid.shutdown)
  }


  def rsyncPlugins(hosts: GenSeq[String] = ips) = {
    command(s"mkdir -p ${instDirs.globalLocation}/cm-well/app/ws/plugins/sg-engines/", hosts, false)
    rsync(s"plugins/", s"${instDirs.globalLocation}/cm-well/app/ws/plugins/sg-engines/", hosts)
  }
}

