name := "cmwell-dc"


packMain := Map("dc-standalone" -> "cmwell.dc.stream.MainStandAlone")
packGenerateWindowsBatFile := false
packExtraClasspath := Map("dc-standalone" -> Seq("${PROG_HOME}"))
val jvmOptsForDcStandAloneUsingPack = Seq(
  "-Xmx1024M",
  "-XX:+UseG1GC",
  "-Dfile.encoding=UTF-8",
  "-Dcom.sun.management.jmxremote.port=7293",
  "-Dcmwell.dc.target=$2",
  "-Duser.timezone=GMT0",
  "-Dcom.sun.management.jmxremote.authenticate=false",
  "-Dcom.sun.management.jmxremote.ssl=false",
  "-verbose:gc",
  "-XX:+PrintGCDetails",
  "-XX:+PrintGCDateStamps",
  "-XX:+PrintHeapAtGC",
  "-XX:+PrintTenuringDistribution",
  "-XX:+PrintGCApplicationConcurrentTime",
  "-XX:+PrintGCApplicationStoppedTime",
  "-XX:+PrintPromotionFailure",
  "-XX:PrintFLSStatistics=1",
  "-Xloggc:logs/gc.log",
  "-XX:+UseGCLogFileRotation",
  "-XX:NumberOfGCLogFiles=9",
  "-XX:GCLogFileSize=10M"
)
packJvmOpts := Map("dc-standalone" -> jvmOptsForDcStandAloneUsingPack)

libraryDependencies ++= {
	val dm = dependenciesManager.value
	Seq(
		dm("org.rogach", "scallop"),
		dm("ch.qos.logback", "logback-classic"),
		dm("com.github.andrewoma.dexx", "collection"),
		dm("com.typesafe", "config"),
		dm("com.typesafe.akka", "akka-stream"),
		dm("com.typesafe.akka", "akka-http"),
//    dm("com.typesafe.akka", "akka-cluster"),
		dm("com.typesafe.akka", "akka-actor"),
//dm("com.typesafe.akka", "akka-cluster-tools"),
		dm("com.typesafe.akka", "akka-slf4j"),
		dm("com.typesafe.akka", "akka-stream-contrib")
			.exclude("org.slf4j", "slf4j-log4j12")
			.exclude("log4j", "log4j"),
		dm("com.typesafe.scala-logging", "scala-logging"),
		dm("joda-time", "joda-time"),
		dm("uk.org.lidalia","sysout-over-slf4j"),
		dm("org.apache.jena", "apache-jena-libs")
			.exclude("commons-logging", "commons-logging")
			.exclude("org.slf4j", "slf4j-log4j12")
			.exclude("log4j", "log4j")
	)
}

//sbt native packager configuration
mappings in Universal += (packResourceDir.value.keys.head / "logback.xml") -> "conf/logback.xml"
scriptClasspath in bashScriptDefines ~= (cp => "../conf" +: cp)
mainClass in Compile := Some("cmwell.dc.stream.MainStandAlone")
val jvmOptsForDcStandAloneUsingNativePackager = Seq(
	"-J-Xmx1024M",
	"-J-XX:+UseG1GC",
	"-Dfile.encoding=UTF-8",
	"-Dcom.sun.management.jmxremote.port=7293",
	"-Duser.timezone=GMT0",
	"-Dcom.sun.management.jmxremote.authenticate=false",
	"-Dcom.sun.management.jmxremote.ssl=false",
	"-J-verbose:gc",
	"-J-XX:+PrintGCDetails",
	"-J-XX:+PrintGCDateStamps",
	"-J-XX:+PrintHeapAtGC",
	"-J-XX:+PrintTenuringDistribution",
	"-J-XX:+PrintGCApplicationConcurrentTime",
	"-J-XX:+PrintGCApplicationStoppedTime",
	"-J-XX:+PrintPromotionFailure",
	"-J-XX:PrintFLSStatistics=1",
	"-J-Xloggc:logs/gc.log",
	"-J-XX:+UseGCLogFileRotation",
	"-J-XX:NumberOfGCLogFiles=9",
	"-J-XX:GCLogFileSize=10M"
)
javaOptions in Universal ++= jvmOptsForDcStandAloneUsingNativePackager

fullTest := (test in Test).value

unmanagedResources in Test += packResourceDir.value.keys.head / "application.conf"