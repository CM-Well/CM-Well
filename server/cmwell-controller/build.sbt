import sbt.Package.ManifestAttributes

packAutoSettings

name := "cmwell-controller"

packageOptions := Seq(ManifestAttributes(
  ("Agent-Class", "cmwell.ctrl.agents.MetricsAgent"),
  ("Premain-Class", "cmwell.ctrl.agents.MetricsAgent")))

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.akka", "akka-cluster"),
    dm("com.typesafe.akka", "akka-cluster-tools"),
    dm("com.typesafe", "config"),
    dm("com.typesafe.play", "play-json"),
    dm("org.hdrhistogram", "HdrHistogram"),
    dm("io.spray", "spray-can"),
    dm("io.spray", "spray-routing"),
    dm("io.spray", "spray-json"))
}

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", "cxf", "bus-extensions.txt") => MergeStrategy.concat
//  case "application.conf" => MergeStrategy.concat
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}
//
////if you only have 1 main class, you should configure it in META-INF/MANIFEST.MF
//mainClass in oneJar := Some("cmwell.ctrl.server.CtrlServer")
//
////every artifact we want to build regularly should be added to packagedArtifacts...
//artifact in oneJar := Artifact(moduleName.value, "executable")

fullTest := (test in Test).value