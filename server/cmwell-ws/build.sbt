

//import NativePackagerKeys._

name := "cmwell-ws"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    guice,
    ws,
    dm("com.typesafe.play","play-json"),
    dm("com.typesafe.akka","akka-http")
      .exclude("com.typesafe.akka","akka-slf4j"),
    dm("com.typesafe.akka", "akka-stream-kafka").exclude("org.apache.kafka", "kafka-clients"),
    dm("com.typesafe.akka","akka-slf4j"),
    dm("ch.qos.logback","logback-classic"),
    dm("org.slf4j", "log4j-over-slf4j"),
    dm("com.ning","async-http-client")
      .exclude("io.netty","netty")
      .exclude("com.google.guava","guava")
      .exclude("org.slf4j","slf4j-api"),
    dm("com.github.andrewoma.dexx","collection"),
    dm("com.google.guava","guava"),
    dm("com.google.code.findbugs","jsr305"),
    dm("com.typesafe.play","play")
      .exclude("com.typesafe.akka","akka-slf4j")
      .exclude("org.slf4j","slf4j-api")
      .exclude("com.fasterxml.jackson.core","jackson-core")
      .exclude("com.ning","async-http-client")
      .exclude("commons-logging","commons-logging")
      .exclude("org.apache.httpcomponents","httpclient")
      .exclude("org.apache.httpcomponents","httpcore")
      .exclude("org.slf4j","jul-to-slf4j"),
    dm("com.typesafe.play","filters-helpers")
      .exclude("com.typesafe.akka","akka-slf4j")
      .exclude("com.typesafe.play","play"),
    dm("com.typesafe.play","play-test")
      .exclude("com.typesafe.akka","akka-slf4j")
      .exclude("com.typesafe.play","play")
      .exclude("com.google.code.findbugs","jsr305")
      .exclude("com.google.guava","guava")
      .exclude("org.scala-lang","scala-library")
      .exclude("commons-logging","commons-logging"),
    dm("com.typesafe.play","play-ws")
      .exclude("com.typesafe.akka","akka-slf4j"),
    dm("com.typesafe.play","play-cache")
      .exclude("com.typesafe.akka","akka-slf4j"), // required for pac4j
    dm("com.fasterxml.jackson.core","jackson-core"),
    dm("joda-time","joda-time"),
    dm("org.apache.httpcomponents","httpclient")
      .exclude("commons-logging","commons-logging"),
    dm("org.apache.httpcomponents","httpcore"),
    dm("org.apache.jena","apache-jena-libs")
      .exclude("commons-logging","commons-logging")
      .exclude("org.slf4j","slf4j-log4j12")
      .exclude("log4j","log4j"),
    dm("org.slf4j","slf4j-api"),
    dm("org.slf4j","jcl-over-slf4j"),
    dm("org.slf4j","jul-to-slf4j"),
    dm("org.yaml","snakeyaml"),
    dm("xml-apis","xml-apis"),
    dm("com.github.t3hnar", "scala-bcrypt"),
    dm("com.jason-goodwin", "authentikat-jwt"),
    dm("org.apache.kafka", "kafka-clients"),
    dm("org.apache.kafka", "kafka")
      .exclude("org.slf4j","slf4j-log4j12")
      .exclude("log4j","log4j"),
    dm("uk.org.lidalia","sysout-over-slf4j"),
  dm("org.openrdf.sesame", "sesame-model"),
  dm("org.openrdf.sesame", "sesame-queryalgebra-evaluation"),
  dm("org.openrdf.sesame", "sesame-repository-api"),
  dm("org.openrdf.sesame", "sesame-repository-manager"),
  dm("org.openrdf.sesame", "sesame-repository-http"),
  dm("org.openrdf.sesame", "sesame-repository-sparql"),
  dm("org.openrdf.sesame", "sesame-repository-sail"),
  dm("org.openrdf.sesame", "sesame-sail-api"),
  dm("org.openrdf.sesame", "sesame-sail-memory"),
  dm("org.openrdf.sesame", "sesame-queryresultio-sparqljson"),
  dm("org.openrdf.sesame", "sesame-queryresultio-text"),
  dm("io.netty", "netty-transport-native-unix-common"),
  dm("org.scala-lang", "scala-compiler"),
  dm("org.openrdf.sesame", "sesame-sail-base"),
  dm("io.circe","circe-core"),
  dm("io.circe","circe-generic"),
  dm("io.circe","circe-parser")
  )
}

testListeners := Seq.empty[TestReportListener]

javacOptions in Test += "-DftsService.default.timeout=10"

mappings in Universal += {
  val f = (assembly in LocalProject("pluginGremlin")).value
  f -> "/plugins/sg-engines/gremlin.jar"
}

fullTest := (test in Test).value

fullClasspath in (Compile,console) += Attributed.blank(sourceDirectory.value / "cws" / "resources")
