name := "cmwell-dao"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("org.slf4j", "slf4j-api"),
    dm("com.typesafe.scala-logging", "scala-logging")
      .exclude("org.slf4j", "slf4j-api"),
    dm("commons-codec", "commons-codec"),
    dm("joda-time", "joda-time"),
    dm("org.joda", "joda-convert"),
    dm("com.datastax.cassandra", "cassandra-driver-core")
      .exclude("io.netty", "netty")
      .exclude("org.slf4j", "slf4j-api")
      .exclude("com.google.guava", "guava"),
    dm("com.google.code.findbugs", "jsr305"),
    dm("com.google.guava", "guava")
  )
}

fullTest := (test in Test).value