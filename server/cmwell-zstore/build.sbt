import cmwell.build.Versions

name := "cmwell-zstore"

cassandraVersion := Versions.cassandra

cassandraCqlInit := ((resourceDirectory in Test).value / "cassandra-cql-test-commands.txt").getAbsolutePath

libraryDependencies += {
  val dm = dependenciesManager.value
  dm("org.codehaus.groovy", "groovy-all") % "test"
}

test in Test := Def.taskDyn {
  val a: Task[String] = startCassandra.taskValue
  val b: Task[Unit] = (test in Test).taskValue
  val c: Task[Unit] = stopCassandra.taskValue
  Def.task {
    ((a doFinally b) doFinally c).value
  }
}.tag(cmwell.build.CMWellCommon.Tags.Cassandra).value

fullTest := (test in Test).dependsOn(fullTest in LocalProject("irw")).value