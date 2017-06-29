name := "cmwell-dc"
packAutoSettings

libraryDependencies ++= {
	val dm = dependenciesManager.value
	Seq(
		dm("ch.qos.logback", "logback-classic"),
		dm("com.github.andrewoma.dexx", "collection"),
		dm("com.typesafe", "config"),
		dm("com.typesafe.akka", "akka-stream"),
		dm("com.typesafe.akka", "akka-http-core"),
		dm("com.typesafe.akka", "akka-cluster"),
		dm("com.typesafe.akka", "akka-actor"),
		dm("com.typesafe.akka", "akka-cluster-tools"),
		dm("com.typesafe.akka", "akka-slf4j"),
		dm("com.typesafe.akka", "akka-stream-contrib")
			.exclude("org.slf4j", "slf4j-log4j12")
			.exclude("log4j", "log4j"),
		dm("com.typesafe.scala-logging", "scala-logging"),
		dm("io.spray", "spray-client"),
		dm("io.spray", "spray-json"),
		dm("joda-time", "joda-time"),
		dm("net.logstash.logback", "logstash-logback-encoder"),
		dm("org.apache.jena", "apache-jena-libs")
			.exclude("commons-logging", "commons-logging")
			.exclude("org.slf4j", "slf4j-log4j12")
			.exclude("log4j", "log4j")
	)
}

mappings in oneJar ++= {
	(unmanagedResources in Compile).value.find(_.getName == "logback.xml") match {
		case Some(f) => Seq((f,"logback.xml"))
		case None => Nil
	}
}

mainClass in oneJar := Some("cmwell.dc.stream.MainStandAlone")

artifact in oneJar := Artifact(moduleName.value, "selfexec")

fullTest := (test in Test).value

unmanagedResources in Test += packResourceDir.value.keys.head / "application.conf"