name := "cmwell-fts-ng"

//Test / Keys.fork := true //the test will run in a spawned JVM (default is to run in the same JVM) -> this is because the tests use cmwell.util.jmx.package$.jmxRegister

libraryDependencies ++= {
	val dm = dependenciesManager.value
	Seq(
		dm("org.elasticsearch.client", "transport"),
		dm("org.slf4j", "log4j-over-slf4j"),
		dm("com.fasterxml.jackson.core", "jackson-core"),
		dm("com.google.guava", "guava"),
		dm("com.google.code.findbugs", "jsr305"),
		dm("com.typesafe", "config"),
		dm("com.spatial4j", "spatial4j"),
		dm("org.slf4j","jcl-over-slf4j"),
		dm("org.apache.logging.log4j", "log4j-to-slf4j"),
    dm("net.lingala.zip4j", "zip4j") % Test)
}

Test / test := Def.task((Test / test).value).tag(cmwell.build.CMWellCommon.Tags.ES).value
