import CMWellCommon.Tags

packAutoSettings

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
    dm("com.typesafe.akka", "akka-stream-kafka").exclude("org.apache.kafka", "kafka-clients"),
    dm("com.typesafe.akka", "akka-agent"),
    dm("com.typesafe.akka", "akka-slf4j"),
    dm("net.jpountz.lz4", "lz4"),
    dm("org.elasticsearch", "elasticsearch"),
    dm("nl.grons", "metrics-scala"),
    dm("org.elasticsearch", "metrics-elasticsearch-reporter"),
    dm("org.apache.kafka", "kafka")
      .exclude("org.slf4j", "slf4j-log4j12")
      .exclude("log4j", "log4j"),
    dm("org.codehaus.groovy", "groovy-all"),
    dm("uk.org.lidalia","sysout-over-slf4j")
  )
}

cassandraVersion := Versions.cassandra

kafkaVersion := Versions.kafka

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

test in Test := Def.taskDyn {
  val a: Task[Unit] = startCassandraAndKafka.taskValue
  val b: Task[Unit] = (test in Test).taskValue
  val c: Task[Unit] = stopCassandraAndKafka.taskValue
  Def.task {
    ((a doFinally b) doFinally c).value
  }
}.tag(Tags.ES,Tags.Cassandra,Tags.Grid,Tags.Kafka).value

fullTest := (test in Test).dependsOn(fullTest in LocalProject("irw"),fullTest in LocalProject("imp"),fullTest in LocalProject("fts"),fullTest in LocalProject("zstore")).value

unmanagedResources in Test += packResourceDir.value.keys.head / "logback.xml"
