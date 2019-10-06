name := "cmwell-sparql-triggered-processor-agent"

libraryDependencies += {
  val dm = dependenciesManager.value
  dm("net.jcazevedo","moultingyaml")
  dm("io.circe","circe-core")
  dm("io.circe","circe-generic")
  dm("io.circe","circe-parser")
//  dm("io.circe","circe-optics")
}
