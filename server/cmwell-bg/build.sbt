import cmwell.build.{Versions,CMWellCommon}, CMWellCommon.Tags

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
    dm("com.typesafe.akka", "akka-stream-kafka").exclude("org.apache.kafka", "kafka-clients"),
    dm("com.typesafe.akka", "akka-agent"),
    dm("com.typesafe.akka", "akka-slf4j"),
    dm("org.lz4", "lz4-java"),
    dm("org.elasticsearch", "elasticsearch"),
//    dm("nl.grons", "metrics-scala"),
    dm("nl.grons", "metrics4-scala"),
    dm("nl.grons", "metrics4-akka_a25"),
    dm("nl.grons", "metrics4-scala-hdr"),
    dm("org.elasticsearch", "metrics-elasticsearch-reporter"),
    dm("org.apache.kafka", "kafka")
      .exclude("org.slf4j", "slf4j-log4j12")
      .exclude("log4j", "log4j"),
    dm("uk.org.lidalia","sysout-over-slf4j"),
//    dm("io.netty", "netty-transport"),
    dm("net.lingala.zip4j", "zip4j") % Test
  )
}

cassandraVersion := Versions.cassandra

kafkaVersion := Versions.kafka

zookeeperVersion := Versions.zookeeper

cassandraCliInit := "NO_CLI_COMMANDS_SUPPLIED"

cassandraCqlInit := ((resourceDirectory in Test).value / "cassandra-cql-commands.txt").absolutePath

val startCassandraAndKafka = Def.task[Unit] {
  Def.task(startKafka.value).value
  startCassandra.value
}

val stopCassandraAndKafka = Def.task[Unit] {
  stopCassandra.value
  Def.task(stopKafka.value).value
}

//test in Test := Def.taskDyn {
//  val a: Task[Unit] = startCassandraAndKafka.taskValue
//  val b: Task[Unit] = (test in Test).taskValue
//  val c: Task[Unit] = stopCassandraAndKafka.taskValue
//  Def.task {
//    ((a doFinally b) doFinally c).value
//  }
//}.tag(Tags.ES,Tags.Cassandra,Tags.Grid,Tags.Kafka).value

fullTest := (test in Test).dependsOn(fullTest in LocalProject("fts"),fullTest in LocalProject("zstore")).value

unmanagedResources in Test += packResourceDir.value.keys.head / "logback.xml"
