import CMWellBuild.autoImport._

dataFolder := baseDirectory.value / "data"

name := "cmwell-stortill"

libraryDependencies ++= {
  val dm = dependenciesManager.value
  Seq(
    dm("com.typesafe.akka", "akka-stream"),
    dm("com.lightbend.akka", "akka-stream-alpakka-cassandra")
  )
}
