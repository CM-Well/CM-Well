import cmwell.build.Versions

name := "cmwell-zstore"

cassandraVersion := Versions.cassandra

cassandraCqlInit := ((resourceDirectory in Test).value / "cassandra-cql-test-commands.txt").getAbsolutePath

/*
test in Test := Def.taskDyn {
  val a: Task[String] = startCassandra.taskValue
  val b: Task[Unit] = (test in Test).taskValue
  val c: Task[Unit] = stopCassandra.taskValue
  Def.task {
    ((a doFinally b) doFinally c).value
  }
}.tag(cmwell.build.CMWellCommon.Tags.Cassandra).value
*/
