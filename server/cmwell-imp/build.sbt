import CMWellCommon.Tags

name := "cmwell-imp"

//Keys.fork in Test := true

libraryDependencies ++= {
	val dm = dependenciesManager.value
	Seq(
		dm("joda-time", "joda-time"),
		dm("org.joda", "joda-convert"),
		dm("com.google.guava", "guava"),
		dm("com.google.code.findbugs", "jsr305"),
		dm("com.ecyrd.speed4j", "speed4j"),
		dm("net.sf.ehcache", "ehcache"),
		dm("org.codehaus.groovy", "groovy-all") % "test",
		dm("com.typesafe.akka", "akka-slf4j") % "test")
}
	
cassandraVersion := Versions.cassandra

cassandraHost := "localhost"

cassandraPort := "9160"

cassandraCqlInit := (cassandraCqlInit in LocalProject("irw")).value  //	file("src/test/resources/cassandra-cql-test-commands").getAbsolutePath

test in Test := Def.taskDyn {
  val a: Task[String] = startCassandra.taskValue
  val b: Task[Unit] = (test in Test).taskValue
  val c: Task[Unit] = stopCassandra.taskValue
  Def.task {
    ((a doFinally b) doFinally c).value
  }
}.tag(Tags.Cassandra,Tags.Grid).value

fullTest := (test in Test).dependsOn(fullTest in LocalProject("irw"),fullTest in LocalProject("zstore")).value

javaOptions in Test += {
	val d = target.value / "data"
	s"-Dcmwell.home=${d.getAbsolutePath}"
}
