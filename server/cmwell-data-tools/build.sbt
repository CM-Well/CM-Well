name := "cmwell-data-tools"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.akka", "akka-stream"),
    dm("com.typesafe.akka", "akka-http"),
    dm("com.typesafe.akka", "akka-slf4j"),
    dm("net.jcazevedo", "moultingyaml"),
    dm("nl.grons", "metrics4-scala"),
    dm("nl.grons", "metrics4-akka_a25"),
    dm("nl.grons", "metrics4-scala-hdr"),
    dm("org.apache.commons", "commons-lang3"),
    dm("com.typesafe.scala-logging", "scala-logging"),
    dm("com.typesafe.play", "play-json"),
    dm("com.typesafe.akka", "akka-testkit") % "test",
    dm("com.typesafe.akka", "akka-stream-testkit") % "test",
    dm("com.github.tomakehurst", "wiremock") % "test" jar()
  )
}
