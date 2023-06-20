import scala.concurrent.Promise
import scala.util.Try
import cmwell.build.{Versions,CMWellCommon}, CMWellCommon.Tags
import scala.sys.process._

name := "cmwell-it"

sbt.Defaults.itSettings

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.github.andrewoma.dexx", "collection") % "it,test",
    dm("com.typesafe.akka", "akka-http") % "it,test",
    dm("com.typesafe.akka", "akka-stream") % "it,test",
    dm("ch.qos.logback", "logback-classic") % "it,test",
    (dm("com.thaiopensource", "jing") % "it,test")
      .exclude("xml-apis", "xml-apis"),
    dm("junit", "junit") % "it,test",
    dm("org.apache.httpcomponents", "httpclient") % "it,test",
    dm("org.apache.httpcomponents", "httpcore") % "it,test",
    (dm("org.apache.jena", "apache-jena-libs") % "it,test")
      .exclude("commons-logging", "commons-logging")
      .exclude("org.slf4j", "slf4j-log4j12")
      .exclude("log4j", "log4j"),
    dm("org.scalatest", "scalatest") % "it,test",
    dm("org.slf4j", "jcl-over-slf4j") % "it,test",
    dm("org.slf4j", "jul-to-slf4j") % "it,test",
    dm("org.slf4j", "log4j-over-slf4j") % "it,test",
    dm("org.slf4j", "slf4j-api") % "it,test",
    dm("xerces", "xercesImpl") % "it,test",
    dm("xml-apis", "xml-apis") % "it,test")
}

IntegrationTest / logBuffered := false

IntegrationTest / fork := true

IntegrationTest / javaOptions += {
  val targetDir = (IntegrationTest / target).value
  val cmwHome = targetDir / "temp"
  s"-Dcmwell.home=${cmwHome.getAbsolutePath}"
}

IntegrationTest / managedResourceDirectories ++= Seq(
  (IntegrationTest / target).value / "conf_files",
  (LocalProject("ws") / packResourceDir).value.keys.head
)

//IntegrationTest / managedResources ++= {
//  /*val orig = */
////  val dest = (IntegrationTest / target).value / "conf_files" / "ws" / "application.conf"
//  //  sbt.IO.copyFile(orig,dest)
//  //  dest
//  (LocalProject("ws") / configSettingsResource).value
//}

IntegrationTest / unmanagedResources += {
  val cons = (LocalProject("cons") / baseDirectory).value
  cons / "app" / "resources" / "meta_ns_prefixes_snapshot_infotons.nt"
}

def jps(): Array[(String,String)] =
  ("jps -l" !!)
    .split('\n')
    .map(p => p.splitAt(p.indexOf(' ')))
    .map{case (pid,clazz) => (pid, clazz.trim)}

def installCmwell = Def.taskDyn[Array[(String,String)]] {
  val targetDir = (IntegrationTest / target).value
  val log = streams.value.log
  val pe = (LocalProject("cons") / peScript).value
  Def.task[Array[(String,String)]] {
    launchCmwell(targetDir,log,pe)
  }.tag(Tags.IntegrationTests)
}

def launchCmwell(targetDir: File, log: Logger, pe: File): Array[(String,String)] = {
  log.warn("DO NOT execute Scala REPL nor Cassandra nor Elasticsearch nor Play's Netty server nor cmwell-bg nor kafka nor zk until integration tests are over!")
  val arr = jps()
  log.info("storing PIDs of processes to preserve before invoking CM-Well process:" + arr.map(t => t._1 + "\t" + t._2).mkString("\n\t", "\n\t", ""))
  log.info(s"executing ${pe.getAbsolutePath}")
  Process(s"${pe.getAbsolutePath} $targetDir") ! log
  log.info("pe processes are up. waiting (10 sec) for meta & spa to index...")
  Thread.sleep(10000)
  arr
}


Test / parallelExecution := true

IntegrationTest / testOptions ++= {

  val log = streams.value.log
  val pescript = (LocalProject("cons") / peScript).value

  var oldJps: Array[(String,String)] = Array.empty

  Seq(Tests.Setup(() => {
    oldJps = launchCmwell((IntegrationTest / target).value,log,pescript)
    log.info("starting tests")
  }),
  Tests.Cleanup(() => {
    log.info("going to kill all cm-well process")
    jps().filterNot(oldJps.contains).foreach {
      case (pid, clazz) => if (Set(
        "cmwell.bg.Runner",
        "cmwell.crashableworker.WorkerMain",
        "cmwell.ctrl.server.CtrlServer",
        "cmwell.dc.stream.Main",
        "kafka.Kafka",
        "org.apache.cassandra.service.CassandraDaemon",
        "org.elasticsearch.bootstrap.Elasticsearch",
        "org.apache.zookeeper.server.quorum.QuorumPeerMain",
        "play.core.server.ProdServerStart",
        "scala.tools.nsc.MainGenericRunner").exists(clazz.contains(_))) s"kill -9 $pid" !
    }
  }))
}

ThisProject / itScalastyle := (ThisProject / IntegrationTest / scalastyle).toTask("").value

IntegrationTest / test := Def.task {
  itScalastyle.value
  (IntegrationTest / test).value
}.tag(Tags.ES,Tags.Cassandra,Tags.Kafka,Tags.Grid).value
