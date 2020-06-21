import CMWellBuild.autoImport._

name := "cmwell-domain-ng"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("ch.qos.logback", "logback-classic"),
    dm("com.typesafe.scala-logging", "scala-logging")
      .exclude("org.slf4j", "slf4j-api"),
    dm("commons-codec", "commons-codec"),
    dm("joda-time", "joda-time"),
    dm("org.joda", "joda-convert"),
    dm("org.slf4j", "slf4j-api"),
    dm("org.scalacheck", "scalacheck") % "test",
    dm("org.slf4j", "jcl-over-slf4j"),
    dm("org.scalacheck","scalacheck") % "domainUtil"
  )
}
