import cmwell.build.{Versions,CMWellCommon}, CMWellCommon.Tags

name := "cmwell-irw"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("org.slf4j", "slf4j-api"),
//    dm("commons-codec", "commons-codec"),
    dm("joda-time", "joda-time"),
    dm("org.joda", "joda-convert"),
    dm("com.lightbend.akka", "akka-stream-alpakka-cassandra"),
//    dm("com.datastax.cassandra", "cassandra-driver-core")
//      .exclude("io.netty", "netty")
//      .exclude("org.slf4j", "slf4j-api")
//      .exclude("com.google.guava", "guava"),
//    dm("com.google.code.findbugs", "jsr305"),
    dm("com.google.guava", "guava"),
//    dm("io.netty", "netty"),
    dm("org.slf4j", "log4j-over-slf4j") % "test")
//    dm("com.whisk", "docker-testkit-scalatest") % "test",
//    dm("com.whisk", "docker-testkit-impl-docker-java") % "test")
}

libraryDependencies += "com.dimafeng" %% "testcontainers-scala" % "0.22.0" % "test"

cassandraVersion := Versions.cassandra
	
cassandraCqlInit := ((resourceDirectory in Test).value / "cassandra-cql-test-commands.txt").getAbsolutePath

test in Test := Def.taskDyn {
  val a: Task[String] = startCassandra.taskValue
  val b: Task[Unit] = (test in Test).taskValue
  val c: Task[Unit] = stopCassandra.taskValue
  Def.task {
    ((a doFinally b) doFinally c).value
  }
}.tag(Tags.Cassandra).value

fullTest := (test in Test).value