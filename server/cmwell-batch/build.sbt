name := "cmwell-batch"

packAutoSettings

libraryDependencies ++= {
	val dm = dependenciesManager.value
	Seq(
		dm("com.typesafe.scala-logging", "scala-logging"),
		dm("com.typesafe", "config"),
		dm("net.logstash.logback", "logstash-logback-encoder"),
		dm("ch.qos.logback", "logback-classic"),
		dm("com.typesafe.akka", "akka-slf4j"),
		dm("uk.org.lidalia","sysout-over-slf4j")
	)
}

mappings in oneJar ++= {
	(unmanagedResources in Compile).value.find(_.getName == "logback.xml") match {
		case Some(f) => Seq((f,"logback.xml"))
		case None => Nil
	}
}

mainClass in oneJar := Some("cmwell.batch.boot.Runner")

artifact in oneJar := Artifact(moduleName.value, "selfexec")

fullTest := (test in Test).value
