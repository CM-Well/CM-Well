import cmwell.build.Versions
import scala.sys.process._

name := "cmwell-common"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.play", "play-json"),
    dm("ch.qos.logback", "logback-classic"),
    dm("com.ecyrd.speed4j", "speed4j"),
    dm("com.fasterxml.jackson.core", "jackson-core"),
    dm("com.google.code.findbugs", "jsr305"),
    dm("com.google.guava", "guava"),
    dm("com.jcraft", "jsch"),
    dm("com.ning", "async-http-client")
      .exclude("io.netty", "netty")
      .exclude("org.slf4j", "slf4j-api"),
    dm("com.typesafe", "config"),
    dm("com.typesafe.akka", "akka-actor"),
    dm("com.typesafe.akka", "akka-http"),
    dm("com.typesafe.akka", "akka-stream"),
    dm("commons-io", "commons-io"),
//    dm("io.netty", "netty-common"),
    dm("io.dropwizard.metrics","metrics-jmx"),
    dm("nl.grons", "metrics4-scala"),
    dm("nl.grons", "metrics4-akka_a25"),
    dm("nl.grons", "metrics4-scala-hdr"),
    dm("org.apache.commons", "commons-compress"),
    dm("org.apache.tika", "tika-parsers")
      .exclude("javax.ws.rs", "javax.ws.rs-api") //this exclude solves the "packaging.type" error. Use the new jakarta artifact instead.
      .exclude("org.jdom", "jdom")
      .exclude("commons-logging", "commons-logging")
      .exclude("commons-logging", "commons-logging-api"),
    dm("jakarta.ws.rs","jakarta.ws.rs-api"), //instead of javax.ws.rs-api
    dm("org.codehaus.plexus", "plexus-archiver")
      .exclude("org.codehaus.plexus", "plexus-container-default")
      .exclude("commons-logging", "commons-logging-api")
      .exclude("log4j", "log4j")
      .exclude("com.google.collections", "google-collections"),
    dm("org.codehaus.plexus", "plexus-container-default")
      .exclude("org.codehaus.plexus", "plexus-utils")
      .exclude("com.google.collections", "google-collections")
      .exclude("log4j", "log4j")
      .exclude("commons-logging", "commons-logging-api"),
    dm("org.codehaus.plexus", "plexus-utils"),
    dm("org.jdom", "jdom2"),
    dm("org.slf4j", "jcl-over-slf4j"),
    dm("org.slf4j", "slf4j-api"),
    dm("org.yaml", "snakeyaml"),
    dm("org.scala-lang.modules", "scala-xml")
  )
}

//sourceGenerators in Compile += buildInfo.taskValue

val encodingVersion = settingKey[String]("sets encodingVersion number")

encodingVersion := "6"

buildInfoKeys := Seq[BuildInfoKey](
  version,
  scalaVersion,
  sbtVersion,
  "encodingVersion" -> encodingVersion.value,
  "cassandraVersion" -> Versions.cassandra,
  "elasticsearchVersion" -> Versions.elasticsearch,
  "kafkaVersion" -> Versions.kafka,
  "zookeeperVersion" -> Versions.zookeeper,
  "buildMachine" -> {
    val pidAtName = java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    pidAtName.dropWhile(_.isDigit).tail
  }, // computed at project load time
  BuildInfoKey.action("buildTime") {
    (new org.joda.time.DateTime()).toString()
  },
  BuildInfoKey.action("gitCommitVersion") {
    Process("git rev-parse HEAD").lines.head //neat trick from SBT in action book :)
  }, // re-computed each time at compile
  "release" -> cmwell.build.CMWellCommon.release
)

sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "cmwell" / "util" / "build" / "JsonSerializer.scala"
  IO.write(file,
    s"""
       |package cmwell.common.build
       |
       |import java.io.ByteArrayInputStream
       |import cmwell.domain.Infoton
       |import cmwell.common.formats.AbstractJsonSerializer
       |import com.fasterxml.jackson.core.JsonParser
       |
       |object JsonSerializer extends AbstractJsonSerializer {
       |  def encodeInfoton(infoton: Infoton): Array[Byte] =
       |    cmwell.common.formats.JsonSerializer.encodeInfoton(infoton)
       |
       |  def decodeInfoton(in: Array[Byte]): Infoton = {
       |    val bais = new ByteArrayInputStream(in)
       |    val jsonParser = jsonFactory.createParser(bais).enable(JsonParser.Feature.AUTO_CLOSE_SOURCE)
       |    val infoton = cmwell.common.formats.JsonSerializer${encodingVersion.value}.decodeInfotonWithParser(jsonParser)
       |    jsonParser.close
       |    infoton
       |  }
       |}
     """.stripMargin)
  Seq(file)
}.taskValue

buildInfoPackage := "cmwell.util.build"

fullTest := (test in Test).value