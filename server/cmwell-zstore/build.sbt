import cmwell.build.Versions

name := "cmwell-zstore"

cassandraVersion := Versions.cassandra

cassandraCqlInit := ((Test / resourceDirectory).value / "cassandra-cql-test-commands.txt").getAbsolutePath

/*
Test / test := Def.taskDyn {
  val a: Task[String] = startCassandra.taskValue
  val b: Task[Unit] = (Test / test).taskValue
  val c: Task[Unit] = stopCassandra.taskValue
  Def.task {
    ((a doFinally b) doFinally c).value
  }
}.tag(cmwell.build.CMWellCommon.Tags.Cassandra).value
*/
