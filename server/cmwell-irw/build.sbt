import cmwell.build.{Versions,CMWellCommon}, CMWellCommon.Tags

name := "cmwell-irw"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("org.slf4j", "slf4j-api"),
    dm("joda-time", "joda-time"),
    dm("org.joda", "joda-convert"),
    dm("com.lightbend.akka", "akka-stream-alpakka-cassandra"),
    dm("com.google.guava", "guava"),
    dm("org.slf4j", "log4j-over-slf4j") % "test")
}

cassandraVersion := Versions.cassandra
	
cassandraCqlInit := ((Test / resourceDirectory).value / "cassandra-cql-test-commands.txt").getAbsolutePath

/* - It was used when using the cassandra plugin - not needed anymore when using docker testing
Test / test := Def.taskDyn {
  val a: Task[String] = startCassandra.taskValue
  val b: Task[Unit] = (Test / test).taskValue
  val c: Task[Unit] = stopCassandra.taskValue
  Def.task {
    ((a doFinally b) doFinally c).value
  }
}.tag(Tags.Cassandra).value
*/
