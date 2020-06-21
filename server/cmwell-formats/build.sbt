import CMWellBuild.autoImport._

name := "cmwell-formats"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.github.andrewoma.dexx", "collection"),
    dm("commons-codec", "commons-codec"),
    dm("com.typesafe.play", "play-json"),
    dm("joda-time", "joda-time"),
    dm("org.apache.abdera", "abdera-parser")
      .exclude("commons-logging", "commons-logging"),
    dm("org.apache.abdera", "abdera-extensions-opensearch")
      .exclude("commons-logging", "commons-logging")
      .exclude("commons-httpclient", "commons-httpclient"),
    dm("org.apache.commons", "commons-csv"),
    dm("org.apache.jena", "apache-jena-libs")
      .exclude("commons-logging", "commons-logging")
      .exclude("org.slf4j", "slf4j-log4j12")
      .exclude("log4j", "log4j")
  )
}
