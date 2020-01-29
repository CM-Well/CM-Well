import CMWellBuild.autoImport._
import sbt._

name := "cmwell-bg"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe", "config"),
    dm("com.typesafe.scala-logging", "scala-logging"),
    dm("ch.qos.logback", "logback-classic"),
    dm("org.slf4j", "log4j-over-slf4j"),
    dm("com.typesafe.akka", "akka-stream"),
    dm("com.typesafe.akka", "akka-stream-contrib"),
    dm("com.typesafe.akka", "akka-stream-testkit") % "test",
    dm("com.typesafe.akka", "akka-stream-kafka")
      .exclude("org.apache.kafka", "kafka-clients"),
    dm("com.typesafe.akka", "akka-agent"),
    dm("com.typesafe.akka", "akka-slf4j"),
    dm("org.lz4", "lz4-java"),
    dm("org.elasticsearch.client", "transport"),
    dm("nl.grons", "metrics4-scala"),
    dm("nl.grons", "metrics4-akka_a25"),
    dm("nl.grons", "metrics4-scala-hdr"),
    dm("org.elasticsearch", "metrics-elasticsearch-reporter"),
    dm("org.apache.kafka", "kafka")
      .exclude("org.slf4j", "slf4j-log4j12")
      .exclude("log4j", "log4j"),
    dm("uk.org.lidalia","sysout-over-slf4j"),
    dm("net.lingala.zip4j", "zip4j") % Test
  )
}

unmanagedResources in Test += packResourceDir.value.keys.head / "logback.xml"
