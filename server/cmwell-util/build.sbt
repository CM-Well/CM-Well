name := "cmwell-util"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.play", "play-json"),
    dm("nl.grons", "metrics-scala"),
    dm("ch.qos.logback", "logback-classic"),
    dm("com.ecyrd.speed4j", "speed4j"),
    dm("com.fasterxml.jackson.core", "jackson-core"),
    dm("com.github.andrewoma.dexx", "collection"),
    dm("com.google.code.findbugs", "jsr305"),
    dm("com.google.guava", "guava"),
    dm("com.jcraft", "jsch"),
    dm("com.ning", "async-http-client")
      .exclude("io.netty", "netty")
      .exclude("org.slf4j", "slf4j-api"),
    dm("com.typesafe", "config"),
    dm("com.typesafe.akka", "akka-actor"),
    dm("com.typesafe.akka", "akka-http-core"),
    dm("com.typesafe.akka", "akka-stream"),
    dm("com.typesafe.akka", "akka-testkit") % "test",
    dm("com.typesafe.akka", "akka-stream-testkit") % "test",
    dm("commons-io", "commons-io"),
    dm("io.netty", "netty"),
    dm("org.lz4","lz4-java"),
    dm("org.apache.commons", "commons-compress"),
    dm("org.apache.jena", "jena-arq")
      .exclude("commons-logging", "commons-logging"),
    dm("org.apache.tika", "tika-parsers")
      .exclude("org.jdom", "jdom")
      .exclude("commons-logging", "commons-logging")
      .exclude("commons-logging", "commons-logging-api"),
    dm("org.codehaus.plexus", "plexus-archiver")
      .exclude("org.codehaus.plexus", "plexus-container-default")
      .exclude("commons-logging", "commons-logging-api")
      .exclude("log4j", "log4j")
      .exclude("com.google.collections", "google-collections"),
    dm("org.codehaus.plexus", "plexus-container-default")
      .exclude("org.codehaus.plexus", "plexus-utils")
      .exclude("com.google.collections", "google-collections")
      .exclude("log4j", "log4j")
      .exclude("commons-logging", "commons-logging-api"),
    dm("org.codehaus.plexus", "plexus-utils"),
    dm("org.jdom", "jdom2"),
    dm("org.slf4j", "jcl-over-slf4j"),
    dm("org.slf4j", "slf4j-api"),
    dm("org.yaml", "snakeyaml"),
    dm("org.scala-lang.modules", "scala-xml"),
    dm("com.typesafe.scala-logging", "scala-logging")
      .exclude("org.slf4j", "slf4j-api")
  )
}

//we need tools.jar on classpath, since we use jps...
unmanagedJars in Compile ~= (_ :+ Attributed.blank(file(System.getProperty("java.home").dropRight(3)+"lib/tools.jar")))

fullTest := (test in Test).value