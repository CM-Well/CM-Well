import sbt.Package.ManifestAttributes

name := "cmwell-controller"

packageOptions := Seq(ManifestAttributes(
  ("Agent-Class", "cmwell.ctrl.agents.MetricsAgent"),
  ("Premain-Class", "cmwell.ctrl.agents.MetricsAgent")))

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.scala-logging", "scala-logging"),
    dm("ch.qos.logback", "logback-classic"),
    dm("com.typesafe.akka", "akka-slf4j"),
    dm("com.typesafe.akka", "akka-cluster"),
    dm("com.typesafe.akka", "akka-cluster-tools"),
    dm("com.typesafe", "config"),
    dm("com.typesafe.play", "play-json"),
    dm("org.hdrhistogram", "HdrHistogram"),
    dm("uk.org.lidalia", "sysout-over-slf4j")
  )
}

fullTest := (test in Test).value