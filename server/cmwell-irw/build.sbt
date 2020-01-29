import CMWellBuild.autoImport._

name := "cmwell-irw"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("org.slf4j", "slf4j-api"),
    dm("joda-time", "joda-time"),
    dm("org.joda", "joda-convert"),
    dm("com.lightbend.akka", "akka-stream-alpakka-cassandra"),
    dm("com.google.guava", "guava"),
    dm("org.slf4j", "log4j-over-slf4j") % "test")
}
