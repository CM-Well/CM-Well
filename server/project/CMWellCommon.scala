object CMWellCommon {

  val release = "Atom"

  object CommonTags {
    val ES = sbt.Tags.Tag("elasticsearch")
    val Cassandra = sbt.Tags.Tag("cassandra")
    val Kafka = sbt.Tags.Tag("kafka")
    val Grid = sbt.Tags.Tag("grid")
    val IntegrationTests = sbt.Tags.Tag("integrationTests")
  }
}
