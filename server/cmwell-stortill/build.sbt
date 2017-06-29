import sbt.Package.ManifestAttributes

name := "cmwell-stortill"

//assemblySettings
//
//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
//{
//  case "application.conf" => MergeStrategy.concat
//  case x => old(x)
//}
//}
//
//
//mainClass in assembly := Some("cmwell.stortill.DupClener")

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.akka", "akka-stream"),
    dm("com.lightbend.akka", "akka-stream-alpakka-cassandra")
  )
}

mainClass in oneJar := Some("cmwell.stortill.DupClener")

artifact in oneJar := Artifact(moduleName.value, "selfexec")

fullTest := (test in Test).value