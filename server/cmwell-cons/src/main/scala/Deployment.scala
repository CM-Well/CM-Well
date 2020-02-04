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
import java.io.{File, PrintWriter}
import java.util.Date

import scala.util.{Failure, Success}
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ParSeq

/**
  * Created by michael on 12/8/15.
  */
//trait RunnableComponent {
//  val processSignature : String
//  val startScriptLocation : String
//  val startScriptPattern : String = "start.sh"
//  val h : Host
//
//  def start = {
//
//  }
//  def stop
//}

trait DataComponent {
  val componentName: String
  val componentDataDirs: Map[String, Seq[String]]
  val h: Host
  def createDataDirectories(hosts: ParSeq[String]) {
    h.info(s"  creating $componentName data directories")
    componentDataDirs.values.flatten.foreach { dataDir =>
      hosts.par.foreach(host => h.command(s"mkdir -p $dataDir", host, false))
    }
  }

  def clearDataDirecoties(hosts: ParSeq[String]) {
    h.info(s"  clearing $componentName's data directories")
    componentDataDirs.values.flatten.foreach { dataDir =>
      h.command(s"rm -rf ${dataDir}/*", hosts, false)
    }
  }

  def linkDataDirectories(hosts: ParSeq[String]) {
    h.info(s"  linking $componentName data directories")
    componentDataDirs.foreach { dd =>
      for (index <- 1 to dd._2.size) {
        h.command(
          s"test -L ${h.getInstDirs.intallationDir}/data/${ResourceBuilder.getIndexedName(dd._1, index)} || ln -s ${dd
            ._2(index - 1)} ${h.getInstDirs.intallationDir}/data/${ResourceBuilder.getIndexedName(dd._1, index)}",
          hosts,
          false
        )
      }
    }
  }
}

trait LoggingComponent {
  val componentName: String
  val componentMappings: Map[String, Int]
  val h: Host
  def createLoggingDirectories(hosts: ParSeq[String]) {
    h.info(s"  creating $componentName log directories")
    componentMappings.foreach { componentMapping =>
      for (index <- 1 to componentMapping._2) {
        h.command(
          s"mkdir -p ${h.getInstDirs.intallationDir}/log/${ResourceBuilder.getIndexedName(componentMapping._1, index)}",
          hosts,
          false
        )
      }
    }
  }

  object LogLevel {
    private var lvl: String = "INFO"
    def warn = lvl = "WARN"
    def error = lvl = "ERROR"
    def info = lvl = "INFO"
    def debug = lvl = "DEBUG"

    def getLogLevel = lvl
  }
}

trait ConfigurableComponent {
  val componentName: String
  val componentMappings: Map[String, Int]
  val h: Host
  def createConigurationsDirectoires(hosts: ParSeq[String]) {
    h.info(s"  creating $componentName configuration directories")
    componentMappings.foreach { componentMapping =>
      for (index <- 1 to componentMapping._2) {
        h.command(
          s"mkdir -p ${h.getInstDirs.intallationDir}/conf/${ResourceBuilder.getIndexedName(componentMapping._1, index)}",
          hosts,
          false
        )
      }
    }
  }
}

trait UpgradeMethod
// Stopping and starting the component on each machine during the upgrade (won't continue to the next machine until the component is stable).
case object Rolling extends UpgradeMethod
// Stopping the component on every machine before the upgrade and start it on all the machines after the upgrade.
case object NonRolling extends UpgradeMethod
// Stopping and starting the component before the upgrade.
case object PreUpgrade extends UpgradeMethod

trait RunnableComponent {
  def start(hosts: Seq[String])
  def stop(hosts: ParSeq[String])

  def upgradeMethod: UpgradeMethod = Rolling
  def upgradeDependency: Set[ComponentProps] = Set.empty[ComponentProps]
}

abstract class ComponentProps(h: Host, name: String, location: String, hasDate: Boolean) {
  def getName = name
  def getCurrentDateStr = {
    val format = new java.text.SimpleDateFormat("yyyyMMdd_hhmmss")
    val date = new Date()
    format.format(date)
  }
  def isDir = true

  lazy val archiveCommand = {
    osName match {
      case "Mac OS X" => "pax -rwl"
      case _ =>
        if (isDir) "cp -al" else "cp -L"
    }
  }

  lazy val osName = System.getProperty("os.name")

  lazy val packageName = getComponentName(name, location)
  lazy val unpackedName = getUnpackedName(packageName, location)

  def getResolvedName: String = unpackedName match {
    case Some(uname) => uname
    case None        => packageName
  }
  /*      def packageLocation : String = location*/
  def targetLocation: String

  def targetFullPath: String = s"${h.getInstDirs.intallationDir}/$targetLocation"

  def unpackCommand: Option[String]
  def symLinkName: Option[String]
  def getUnpackedName(packageName: String, location: String): Option[String] =
    Some(getTarResName(packageName, location))

  def getTarResName(path: String, location: String): String = // head -2 tail -1 because sometimes the first line is only "./"
    h.command("tar -tf " + s"$location/$path" + " | head -2 | tail -1 | sed 's/\\.\\///' | awk -F '/' '{print $1}'").get.trim

  def getZipResName(path: String, location: String): String =
    h.command("unzip -l " + s"$location/$path" + " | awk '{print $4}' |  awk -F '/' '{print $1}' | head -4 | tail -1")
      .get
      .trim

  def getComponentName(name: String, location: String): String = {
    h.command(s"basename `ls $location/*$name*`").get.trim
  }

  def createSymbolicLink(name: String, symbolicLinkName: String, hosts: ParSeq[String]) = {
    h.command(s"ln -s $targetFullPath/$name $targetFullPath/$symbolicLinkName", hosts, false)
  }

  def relink(linkTo: String, hosts: ParSeq[String]) = {
    h.command(s"test -L $targetFullPath/${symLinkName.get} && rm $targetFullPath/${symLinkName.get}", hosts, false)
    h.command(
      s"test -L $targetFullPath/${symLinkName.get} || ln -s $targetFullPath/$linkTo $targetFullPath/${symLinkName.get}",
      hosts,
      false
    )
  }

  def quickCopy(name: String, hosts: ParSeq[String]) = {

    symLinkName match {
      case Some(sln) => {
        if (osName == "Mac OS X")
          h.command(s"mkdir -p $targetFullPath/$name")
        if (isDir)
          h.command(s"test -d $targetFullPath/$sln/ && $archiveCommand $targetFullPath/$sln/ $targetFullPath/$name",
                    hosts,
                    false)
        else
          h.command(s"test -f $targetFullPath/$sln && $archiveCommand $targetFullPath/$sln $targetFullPath/$name",
                    hosts,
                    false)
      }
      case None => {
        //            if(isDir)
        //              throw new Exception("no symlink")
      }
    }

  }

  def unpackComponent(localLocation: String, name: String, cmd: String) = {
    h.command(s"cd $localLocation; $cmd $name")
  }

  def addPostfixToComponent(remoteLocation: String, name: String, postfix: String, hosts: ParSeq[String]) = {
    h.command(s"mv $remoteLocation/$name $remoteLocation/$name-$postfix", hosts, false)
  }

  def uploadComponent(localLocation: String, target: String, postFix: Option[String], hosts: ParSeq[String]) = {
    val realName = unpackedName match {
      case Some(uname) => uname
      case None        => packageName
    }
    unpackCommand match {
      case Some(cmd) => unpackComponent(location, packageName, cmd)
      case None      => // Do nothing
    }

    val results =
      if (h.command(s"if test -d $localLocation/$realName; then echo 'folder'; else echo 'file'; fi")
            .get
            .equals("folder")) h.rsync(s"$localLocation/$realName/", s"$target/$realName", hosts)
      else h.rsync(s"$localLocation/$realName", s"$target/", hosts)

    if (results.filter(r => r.isFailure).size > 0) throw new Exception("Failed to upload components")

    unpackedName match {
      case Some(uname) => removeUnpacked(location, uname)
      case None        => // Do nothing
    }

    postFix match {
      case Some(pf) => addPostfixToComponent(target, realName, pf, hosts)
      case None     => // Do nothing
    }
  }

  def removeUnpacked(localLocation: String, name: String) = {
    h.command(s"rm -rf $localLocation/$name")
  }

  def redeployComponent(hosts: ParSeq[String] = h.ips.par): String = {
    val dst = targetFullPath
    val dateStr = getCurrentDateStr
    val datePostFix = if (hasDate) Some(dateStr) else None

    val newName = datePostFix match {
      case Some(dpf) => s"$getResolvedName-$dpf"
      case None      => getResolvedName
    }

    quickCopy(getResolvedName, hosts)
    uploadComponent(location, dst, datePostFix, hosts)

    newName
  }

  def deployComponent(hosts: ParSeq[String] = h.ips.par) {
    val dst = targetFullPath
    val dateStr = getCurrentDateStr

    val datePostFix = if (hasDate) Some(dateStr) else None

    uploadComponent(location, dst, datePostFix, hosts)

    val remoteName = (unpackedName, hasDate) match {
      case (Some(uname), true)  => s"$uname-$dateStr"
      case (Some(uname), false) => uname
      case (None, true)         => s"$packageName-$dateStr"
      case (None, false)        => packageName

    }

    symLinkName match {
      case Some(sln) => createSymbolicLink(remoteName, sln, hosts)
      case None      => // Do nothing
    }
  }

  def getUnsyncedHosts(hosts: ParSeq[String] = h.ips.par): ParSeq[String] = {
    val nameToCheck = symLinkName match {
      case Some(symName) =>
        symName
      case None =>
        packageName
    }
    val res = {
      val sb = new StringBuilder
      sb ++= "test -L "
      sb ++= h.getInstDirs.intallationDir
      sb += '/'
      sb ++= targetLocation
      sb += '/'
      sb ++= nameToCheck
      sb ++= " && basename `readlink "
      sb ++= h.getInstDirs.intallationDir
      sb += '/'
      sb ++= targetLocation
      sb += '/'
      sb ++= nameToCheck
      sb += '`'
      h.command(sb.result(), hosts, false)
        .map {
          case Success(str) => str.trim
          case Failure(err) => ""
        }
        .zip(hosts)
    }
    unpackedName match {
      case Some(uName) => res.filter(t => t._1 != uName).map(t => t._2)
      case None        => res.filter(t => t._1 != packageName).map(t => t._2)
    }

  }
}

case class CassandraProps(h: Host)
    extends ComponentProps(h, "cassandra", "components", false)
    with DataComponent
    with LoggingComponent
    with ConfigurableComponent
    with RunnableComponent {
  override val componentName: String = "cassandra"
  override val componentDataDirs: Map[String, Seq[String]] = Map(
    "cas" -> h.getDataDirs.casDataDirs,
    "ccl" -> h.getDataDirs.casCommitLogDirs
  )

  override val componentMappings: Map[String, Int] = Map("cas" -> h.getDataDirs.casDataDirs.size)

  def targetLocation = "app/cas"
  def unpackCommand: Option[String] = Some("tar -xf")
  def symLinkName: Option[String] = Some("cur")

  def start(hosts: Seq[String]): Unit = {
    h.startCassandra(hosts.to(ParSeq))
  }

  def stop(hosts: ParSeq[String]): Unit = {
    h.stopCassandra(hosts)
  }

  override def upgradeDependency: Set[ComponentProps] = Set(JavaProps(h))

  override def createLoggingDirectories(hosts: ParSeq[String]) {
    h.info(s"  creating $componentName log directories")
    componentMappings.foreach { componentMapping =>
        h.command(
          s"mkdir -p ${h.getInstDirs.intallationDir}/log/${componentMapping._1}",
          hosts,
          false
        )
    }
  }

  override def createConigurationsDirectoires(hosts: ParSeq[String]) {
    h.info(s"  creating $componentName configuration directories")
    componentMappings.foreach { componentMapping =>
        h.command(
          s"mkdir -p ${h.getInstDirs.intallationDir}/conf/${componentMapping._1}",
          hosts,
          false
        )
    }
  }

}

case class ElasticsearchProps(h: Host)
    extends ComponentProps(h, "elasticsearch", "components", false)
    with DataComponent
    with LoggingComponent
    with ConfigurableComponent
    with RunnableComponent {
  override val componentName: String = "elasticsearch"
  override val componentDataDirs: Map[String, Seq[String]] = Map(
    "es" -> h.getDataDirs.esDataDirs.filterNot(_.endsWith("master")),
    "es-master" -> h.getDataDirs.esDataDirs.filter{_.endsWith("master")}
  )
  override val componentMappings: Map[String, Int] = Map("es" -> h.getDataDirs.esDataDirs.size, "es-master" -> 1)

  override def upgradeMethod: UpgradeMethod = PreUpgrade

  override def getUnpackedName(packageName: String, location: String): Option[String] = Some(getTarResName(packageName, location))

  def targetLocation = "app/es"
  def unpackCommand : Option[String] = Some("tar -xf")
  def symLinkName : Option[String] = Some("cur")

  def start(hosts: Seq[String]): Unit = {
    h.startElasticsearch(hosts)
  }

  def stop(hosts: ParSeq[String]): Unit = {
    h.stopElasticsearch(hosts)
  }

  override def upgradeDependency: Set[ComponentProps] = Set(JavaProps(h))
}

case class KafkaProps(h: Host)
    extends ComponentProps(h, "kafka", "components", false)
    with DataComponent
    with LoggingComponent
    with ConfigurableComponent
    with RunnableComponent {
  /*      def packageLocation : String = location*/
  override def targetLocation: String = "app/kafka"

  override def unpackCommand: Option[String] = Some("tar -xf")

  override def symLinkName: Option[String] = Some("cur")

  override val componentDataDirs: Map[String, Seq[String]] = Map("kafka" -> h.getDataDirs.kafkaDataDirs)
  override val componentName: String = "kafka"
  override val componentMappings: Map[String, Int] = Map("kafka" -> 1)

  override def stop(hosts: ParSeq[String]): Unit = h.stopKafka(hosts)

  override def start(hosts: Seq[String]): Unit = h.startKafka(hosts.to(ParSeq))

  override def upgradeDependency: Set[ComponentProps] = Set(JavaProps(h))
}

case class ZooKeeperProps(h: Host)
    extends ComponentProps(h, "zookeeper", "components", false)
    with DataComponent
    with LoggingComponent
    with ConfigurableComponent
    with RunnableComponent {
  /*      def packageLocation : String = location*/
  override def targetLocation: String = "app/zookeeper"

  override def unpackCommand: Option[String] = Some("tar -xf")

  override def symLinkName: Option[String] = Some("cur")

  override val componentDataDirs: Map[String, Seq[String]] = Map("zookeeper" -> Seq(h.getDataDirs.zookeeperDataDir))
  override val componentName: String = "zookeeper"
  override val componentMappings: Map[String, Int] = Map("zookeeper" -> 1)

  override def stop(hosts: ParSeq[String]): Unit = h.stopZookeeper(hosts)

  override def start(hosts: Seq[String]): Unit = h.startZookeeper(hosts.to(ParSeq))

  override def upgradeDependency: Set[ComponentProps] = Set(JavaProps(h))
}

case class BgProps(h: Host)
    extends ComponentProps(h, "cmwell-bg", "components", true)
    with LoggingComponent
    with RunnableComponent
    with ConfigurableComponent {
  /*      def packageLocation : String = location*/
  override def targetLocation: String = "app/bg"

  override def unpackCommand: Option[String] = None

  override def symLinkName: Option[String] = None

  override val componentName: String = "cmwell-bg"
  override val componentMappings: Map[String, Int] = Map("bg" -> 1)

  override def stop(hosts: ParSeq[String]): Unit = h.stopBg(hosts)

  override def start(hosts: Seq[String]): Unit = h.startBg(hosts.to(ParSeq))

  override def isDir = false
  override def getUnpackedName(packageName: String, location: String): Option[String] = None

  override def getComponentName(name: String, location: String): String = name

  override def uploadComponent(localLocation: String,
                               target: String,
                               postFix: Option[String],
                               hosts: ParSeq[String]): Any = {}

  override def upgradeMethod: UpgradeMethod = NonRolling
}

case class CtrlProps(h: Host)
    extends ComponentProps(h, "cmwell-controller", "components", true)
    with LoggingComponent
    with ConfigurableComponent
    with RunnableComponent {
  override val componentName: String = "cmwell-controller"
  override val componentMappings: Map[String, Int] = Map("ctrl" -> 1)
  override def uploadComponent(localLocation: String,
                               target: String,
                               postFix: Option[String],
                               hosts: ParSeq[String]): Any = {}
  def targetLocation = "app/ctrl"
  def unpackCommand: Option[String] = None
  def symLinkName: Option[String] = None
  override def getComponentName(name: String, location: String): String = name
  override def isDir = false
  override def getUnpackedName(packageName: String, location: String): Option[String] = None

  def start(hosts: Seq[String]): Unit = {
    h.startCtrl(hosts.to(ParSeq))
  }

  def stop(hosts: ParSeq[String]): Unit = {
    h.stopCtrl(hosts)
  }

  override def upgradeMethod: UpgradeMethod = NonRolling
}

case class DcProps(h: Host)
    extends ComponentProps(h, "cmwell-dc", "components", true)
    with LoggingComponent
    with RunnableComponent {
  override def targetLocation: String = "app/dc"
  override def uploadComponent(localLocation: String,
                               target: String,
                               postFix: Option[String],
                               hosts: ParSeq[String]): Any = {}
  override def unpackCommand: Option[String] = None

  override def symLinkName: Option[String] = None
  override def getComponentName(name: String, location: String): String = name
  override val componentName: String = "cmwell-dc"
  override val componentMappings: Map[String, Int] = Map("dc" -> 1)
  override def isDir = false
  override def getUnpackedName(packageName: String, location: String): Option[String] = None

  def start(hosts: Seq[String]): Unit = {
    h.startDc(hosts.to(ParSeq))
  }

  def stop(hosts: ParSeq[String]): Unit = {
    h.stopDc(hosts)
  }

  override def upgradeMethod: UpgradeMethod = NonRolling
}

case class WebserviceProps(h: Host)
    extends ComponentProps(h, "cmwell-ws", "components", true)
    with LoggingComponent
    with ConfigurableComponent
    with RunnableComponent {
  override val componentName: String = "cmwell-ws"
  override val componentMappings: Map[String, Int] = Map("ws" -> 1)
  override def uploadComponent(localLocation: String,
                               target: String,
                               postFix: Option[String],
                               hosts: ParSeq[String]): Any = {}
  def targetLocation = "app/ws"
  override def isDir = false
  def symLinkName: Option[String] = None
  override def getComponentName(name: String, location: String): String = name
  override def getUnpackedName(packageName: String, location: String): Option[String] = None
  def unpackCommand: Option[String] = None
  override def start(hosts: Seq[String]): Unit = {
    h.startWebservice(hosts.to(ParSeq))
    h.startCW(hosts.to(ParSeq))
  }

  def stop(hosts: ParSeq[String]): Unit = {
    h.stopWebservice(hosts)
    h.stopCW(hosts)
  }
}

case class JavaProps(h: Host) extends ComponentProps(h, "jdk", "components-extras", false) {
  def targetLocation = "app"
  def unpackCommand: Option[String] = Some("tar -xf")
  def symLinkName: Option[String] = Some("java")
  override def getTarResName(path: String, location: String): String =
    h.command("tar -tf " + s"$location/$path" + " | head -1 | awk -F '/' '{print $1}'").get.trim

}

case class Mx4JProps(h: Host) extends ComponentProps(h, "mx4j", "components", false) {
  def targetLocation = "app/tools"
  def unpackCommand: Option[String] = None
  def symLinkName: Option[String] = None

  override def getUnpackedName(packageName: String, location: String): Option[String] = None
}

case class BinsProps(h: Host) extends ComponentProps(h, "bin", ".", false) {
  def targetLocation = ""
  def unpackCommand: Option[String] = None
  def symLinkName: Option[String] = None

  override def deployComponent(hosts: ParSeq[String] = h.ips.par) {
    h.rsync("bin/", s"${h.getInstDirs.intallationDir}/bin/", hosts)
  }

  override def getUnpackedName(packageName: String, location: String): Option[String] = None
}

class Deployment(h: Host) {

  def getCurrentDateStr = {
    val format = new java.text.SimpleDateFormat("yyyyMMdd_hhmmss")
    val date = new Date()
    format.format(date)
  }
  val componentProps: Vector[ComponentProps] = Vector(
    CassandraProps(h),
    ElasticsearchProps(h),
    ZooKeeperProps(h),
    KafkaProps(h),
    BgProps(h),
    WebserviceProps(h),
    BinsProps(h),
    Mx4JProps(h),
    CtrlProps(h),
    DcProps(h)
  ) ++ (if (h.getDeployJava)
          Vector(JavaProps(h))
        else
          Vector.empty[ComponentProps])
  //val componentProps : Vector[ComponentProps] = Vector(ElasticsearchProps)

  def createDirs(hosts: ParSeq[String], components: Seq[Any]): Unit = {
    components.collect {
      case dc: DataComponent =>
        dc.createDataDirectories(hosts)
        dc.linkDataDirectories(hosts)
    }

    components.collect {
      case lc: LoggingComponent =>
        lc.createLoggingDirectories(hosts)
    }

    components.collect {
      case cc: ConfigurableComponent =>
        cc.createConigurationsDirectoires(hosts)
    }
  }

  def createFile(content: String, fileName: String, location: String, hosts: ParSeq[String]) {
    def getMillis = java.lang.System.currentTimeMillis()
    def getRandomString = {
      import scala.util.Random
      val len = 10
      Random.alphanumeric.take(len).mkString
    }

    val tmpName = s"${fileName}___${getMillis}_${getRandomString}"
    val f = new File(tmpName)
    val writer = new PrintWriter(f)
    writer.write(content)
    writer.close
    h.command(s"mkdir -p $location", hosts, false)
    h.rsync(tmpName, s"$location/$fileName" , hosts)
    f.delete()
  }

  def createScript(module: ComponentConf) {
    val confContent = module.mkScript
    if (confContent != null) {
      val retStat = "echo '$?'"

      //val cont = "cd $(dirname -- \"$0\")\n" + "bash -c \"" + confContent.content + ";  if [ $? -ne 0 ]; then " +
      // module.scriptDir + "/" + confContent.fileName + " ; fi \" & "
      val cont = "cd $(dirname -- \"$0\")\n " + s""" ${confContent.content} """

      //command( s"""echo '${cont}' > ${module.scriptDir}/${confContent.fileName}""", module.host, false)

      createFile(cont, confContent.fileName, module.scriptDir, ParSeq(module.host))
      h.command(s"chmod +x ${module.scriptDir}/${confContent.fileName}", module.host, false)
    }
  }

  def createConf(module: ComponentConf) {
    val confContents = module.mkConfig
    if (confContents != null) {
      confContents.foreach { confContent =>
        val dir = confContent.path.getOrElse(module.confDir)
        createFile(confContent.content, confContent.fileName, dir, ParSeq(module.host))
        if (confContent.executable)
          h.command(s"chmod +x $dir/${confContent.fileName}", module.host, false)
      }
    }
  }

  def make(module: ComponentConf): Unit = {
    createScript(module)
    createConf(module)
  }

  def createResources(modules: ParSeq[ComponentConf]) {
    modules.toList.foreach(m => make(m))

  }

  /*

  def createScript(content : String, dir : String , fileName : String, host : String) {
  command(s"""echo \"\"\"$content\"\"\" > $dir/$fileName""" , host, false)
  command(s"chmod +x $dir/$fileName", host, false)
}

def createConf(content : String, dir : String , fileName : String, host : String) {
  command(s"""echo \"\"\"$content\"\"\" > $dir/$fileName""" , host, false)
}

 */

}
