name := "cmwell-batch"


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

fullTest := (test in Test).value
