name := "cmwell-fts-ng"

Keys.fork in Test := true //the test will run in a spawned JVM (default is to run in the same JVM) -> this is because the tests use cmwell.util.jmx.package$.jmxRegister

libraryDependencies ++= {
	val dm = dependenciesManager.value
	Seq(
		dm("org.elasticsearch", "elasticsearch")
			.exclude("org.slf4j", "slf4j-log4j12")
			.exclude("log4j", "log4j")
			.exclude("com.fasterxml.jackson.core", "jackson-core")
			.exclude("com.google.guava", "guava")
			.exclude("org.apache.lucene", "lucene-queryparser")
			.exclude("org.apache.lucene", "lucene-analyzers-common")
			.exclude("org.apache.lucene", "lucene-core") jar(),
		dm("org.elasticsearch.client", "transport"),
		dm("org.slf4j", "log4j-over-slf4j"),
		dm("org.apache.lucene", "lucene-queryparser"),
		dm("org.apache.lucene", "lucene-analyzers-common"),
		dm("org.apache.lucene", "lucene-core"),
		dm("com.fasterxml.jackson.core", "jackson-core"),
		dm("com.google.guava", "guava"),
		dm("com.google.code.findbugs", "jsr305"),
		dm("com.typesafe", "config"),
		dm("com.spatial4j", "spatial4j"),
		dm("org.slf4j","jcl-over-slf4j"))
}

libraryDependencies ++= Seq(
	"net.lingala.zip4j" % "zip4j" % "1.3.2" % Test,
	"pl.allergro.tech" % "embedded-elasticsearch" % "2.4.3" % Test from "https://github.com/israel/embedded-elasticsearch/releases/download/2.4.3/embedded-elasticsearch-2.4.3.jar"
)

test in Test := Def.task((test in Test).value).tag(cmwell.build.CMWellCommon.Tags.ES).value

fullTest := (test in Test).value