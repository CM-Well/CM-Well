
//val pluginVersions = SettingKey[Map[String,String]]("plugin-versions", "set plugin dependency versions")
//
//pluginVersions := Map("play" -> "2.5.17")
//
//sourceGenerators += Def.task {
//  val file = sourceManaged.value / "cmwell" / "build" / "PluginVersions.scala"
//  IO.write(file,
//    s"""
//     |package cmwell.build
//     |
//     |object PluginVersions {
//     |  ${pluginVersions.value.map{
//          case (moduleName,moduleVersion) => moduleName + " = \"" + moduleVersion + "\""
//        }.mkString("val ","\nval ","")}
//     |}
//     """.stripMargin)
//  Seq(file)
//}.taskValue