import java.nio.file.FileAlreadyExistsException

import sbt.LocalProject

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.sys.process._
import cmwell.build.{Versions,CMWellBuild}

name := "cmwell-cons"

unmanagedResources := Seq()

getExternalComponents := {
  val logger = streams.value
  val dm = dependenciesManager.value

  val casM = dm("org.apache.cassandra", "apache-cassandra") artifacts (Artifact("apache-cassandra", "tar.gz", "tar.gz", "bin")) intransitive()
  val casF: scala.concurrent.Future[Seq[java.io.File]] = {
    CMWellBuild.fetchMvnArtifact(casM,scalaVersion.value,scalaBinaryVersion.value,logger.log)
  }

  val esM = dm("org.elasticsearch", "elasticsearch") artifacts (Artifact("elasticsearch", "tar.gz", "tar.gz")) intransitive()
  val esF: scala.concurrent.Future[Seq[java.io.File]] = {
    CMWellBuild.fetchMvnArtifact(esM,scalaVersion.value,scalaBinaryVersion.value,logger.log)
  }

  val mx4jM = dm("mx4j", "mx4j-tools")
  val mx4jF: scala.concurrent.Future[Seq[java.io.File]] = {
    CMWellBuild.fetchMvnArtifact(mx4jM,scalaVersion.value,scalaBinaryVersion.value,logger.log)
  }

  val kafkaF = CMWellBuild.fetchKafka(scalaBinaryVersion.value,Versions.kafka)

  val zkF = CMWellBuild.fetchZookeeper(Versions.zookeeper)

  val bd = baseDirectory.value

  val vecF: scala.concurrent.Future[Vector[(String,java.io.File)]] = for {
    cas   <- casF
    es    <- esF
    mx4j  <- mx4jF
    kafka <- kafkaF
    zk    <- zkF
  } yield {
    val b = Vector.newBuilder[(String,File)]
    b ++= cas.map(file  => file.name -> file)
    b ++= es.map(file   => file.name -> file)
    b ++= mx4j.collect{ case file if file.getName.endsWith(".jar") => file.name -> file}
    b +=  s"kafka-dist-${Versions.kafka}.tgz"       -> kafka
    b +=  s"zookeeper-${Versions.zookeeper}.tar.gz" -> zk
    b.result()
  }

  val files = Await.result(vecF,Duration.Inf)

  val filesBuilder = List.newBuilder[File]
  for ((component,file) <- files) {
    val binaryFile = bd / "app" / "components" / component
    filesBuilder += binaryFile
    if ( binaryFile.exists ) {
      logger.log.info(s"file $component already exists in machine")
    } else {
      logger.log.info(s"copying $file")
      sbt.IO.copyFile(file,binaryFile,true)
    }
  }

  filesBuilder.result()
}

getData := {
  val logger = streams.value
  val baseDir = baseDirectory.value
  val spaDir = (dataFolder in LocalProject("spa") in Compile).value
  val docsDir = baseDir / ".." / ".." / "docs"
  val sysDir = (sourceDirectory in LocalProject("ws") in Compile).value / ".." / "public" / "sys"

  logger.log.info(sysDir.absolutePath)

  logger.log.info("copying data files")
  val data: java.io.File = baseDir / "app" / "data"
  val docs: java.io.File = baseDir / "app" / "docs"
  val sys: java.io.File = baseDir / "app" / "sys"
  sbt.IO.copyDirectory(spaDir, data, true)
  sbt.IO.copyDirectory(docsDir , docs, true)
  sbt.IO.copyDirectory(sysDir , sys, true)
  sbt.IO.delete(data / ".idea")

  refreshAppCacheManifest(baseDir / "app" / "data" / "main" / "cmwell.appcache")

  Seq(data,docs)
}

getCons := {
  val str = streams.value
  val bd = baseDirectory.value
  val cons = (packageBin in Compile).value

  str.log.info(s"cons file to copy is: ${cons.getAbsolutePath}")
  val destination = bd / "app" / "components" / cons.getName
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-cons_"))
  filesToDelete.foreach(sbt.IO.delete)
  sbt.IO.copyFile(cons,destination,preserveLastModified = true)
  destination
}

getWs := {
  val str = streams.value
  val bd = baseDirectory.value
  val ws = (packageBin in LocalProject("ws") in Universal).value

  //make sure to also generate app data & docs
  getData.value

  str.log.info(s"ws file to copy is: ${ws.getAbsolutePath}")
  val destination = bd / "app" / "components" / ws.getName
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-ws_"))
  filesToDelete.foreach(sbt.IO.delete)
  sbt.IO.copyFile(ws,destination,preserveLastModified = true)
  destination
}

//getBg := {
//  val str = streams.value
//  val bd = baseDirectory.value
////  val batch = (oneJar in LocalProject("batch") in oneJar).value
//
//  str.log.info(s"bg file to copy is: ${batch.getAbsolutePath}")
//  val destination = bd / "app" / "components" / batch.getName
//  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-batch_"))
//  filesToDelete.foreach(sbt.IO.delete)
//  sbt.IO.copyFile(batch,destination,preserveLastModified = true)
//  destination
//}

//getDc := {
//  val str = streams.value
//  val bd = baseDirectory.value
//  val dc = (oneJar in LocalProject("dc") in oneJar).value
//
//  str.log.info(s"dc file to copy is: ${dc.getAbsolutePath}")
//  val destination = bd / "app" / "components" / dc.getName
//  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-dc"))
//  filesToDelete.foreach(sbt.IO.delete)
//  sbt.IO.copyFile(dc,destination,preserveLastModified = true)
//  destination
//}

//getCtrl := {
//  val str = streams.value
//  val bd = baseDirectory.value
//  val ctrl = (assembly in LocalProject("ctrl") in Compile).value
//
//  str.log.info(s"crtl file to copy is: ${ctrl.getAbsolutePath}")
//  val destination = bd / "app" / "components" / ctrl.getName
//  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filter(_.getAbsolutePath.contains("cmwell-ctrl"))
//  filesToDelete.foreach(sbt.IO.delete)
//  sbt.IO.copyFile(ctrl,destination,preserveLastModified = true)
//  destination
//}


def packCons(ctrl : File, cons : File , bd : File) = {

  val libDir = ctrl.getAbsoluteFile / "lib"

  val lib = bd / "app" / "cons-lib"

  if(lib.exists()) sbt.IO.delete(lib.listFiles())
  else lib.mkdir()


  libDir.listFiles().foreach {
    file =>
      if(!file.name.contains("apache-cassandra"))
        sbt.IO.copyFile(file,lib / file.getName ,preserveLastModified = true)
  }

  sbt.IO.copyFile(cons , bd / "app" / "cons-lib" / cons.getName , preserveLastModified =  true)
}

def packProject(projName : String, proj : File, bd : File, confDir: File, logger : Logger) = {
  val libDir = proj.getAbsoluteFile / "lib"

  val lib = bd / "app" / "lib"
  val dependencies = bd / "app" / "dependencies"

  if(!lib.exists()) lib.mkdir()

  val depList = libDir.listFiles().map(_.getName).mkString("\n")
  sbt.IO.write(dependencies / projName, depList)


  libDir.listFiles().foreach {
    file =>
      val target = lib / file.getName
      try {
        java.nio.file.Files.createLink(target.toPath, file.toPath)
      } catch {
        case t:FileAlreadyExistsException => // todo: handle handle duplicated jar names.
        case t:Throwable =>
          logger.error(s"Error while creating hard link, will copy instead: $t ${t.getMessage}")
          sbt.IO.copyFile(file,target ,preserveLastModified = true)
      }
  }

  //copy conf files
  proj.listFiles(new FileFilter {
    override def accept(file: File): Boolean = !Set("lib","bin","VERSION","Makefile")(file.getName)
  }).foreach{file =>
    if(file.isDirectory) sbt.IO.copyDirectory(file,confDir / file.getName())
    else {
      sbt.IO.copyFile(file, confDir / file.getName())
    }
  }
}

def refreshAppCacheManifest(appCacheManifestFile: File) = {
  if(appCacheManifestFile.exists()) {
    val newFileContent = sbt.IO.read(appCacheManifestFile).lineStream.map { line =>
      if (line.startsWith("#")) s"# ${Process("git rev-parse HEAD").lineStream.head}-${System.currentTimeMillis}"
      else line
    }.mkString("\n")
    sbt.IO.write(appCacheManifestFile, newFileContent)
  }
}

getGremlin := {
  val str = streams.value
  val bd = baseDirectory.value
  val gremlin = (assembly in LocalProject("pluginGremlin")).value
  val pluginDir = bd / "app" / "plugins"
  if( ! (pluginDir.exists()) ) pluginDir mkdir
  val destination = pluginDir / "gremlin.jar"
  sbt.IO.copyFile(gremlin,destination,preserveLastModified = true)
  destination
}

getLib := {
  val bd = baseDirectory.value

  implicit val logger = streams.value.log

  val lib = bd / "app" / "lib"

  if(lib.exists()) sbt.IO.delete(lib.listFiles())

  val dependencies = bd / "app" / "dependencies"

  if(!lib.exists()) lib.mkdir()
  if(!dependencies.exists()) dependencies.mkdir() else sbt.IO.delete(dependencies.listFiles())

  packProject("ctrl",(pack in LocalProject("ctrl") in pack).value,bd, bd / "app" / "conf" / "ctrl",logger)
  packProject("dc",(pack in LocalProject("dc") in pack).value,bd, bd / "app" / "conf" / "dc",logger)
  packProject("ws",(pack in LocalProject("ws") in pack).value,bd, bd / "app" / "conf" / "ws",logger)
  packProject("bg",(pack in LocalProject("bg") in pack).value,bd, bd / "app" / "conf" / "bg",logger)
  packCons((pack in LocalProject("ctrl") in pack).value, getCons.value , bd)

  lib
}

packageCMWell := {
  val str = streams.value
  val bd = baseDirectory.value
  val cp = (dependencyClasspath in Compile).value

  getGremlin.value

  //get all generated components as well

  getData.value
  getLib.value
  def wanted(f: sbt.File): Boolean = {
    val jars = List("aspectjweaver","mx4j-tools","zookeeper", "kafka")
    val name = f.getName
    name.endsWith("tar.gz") || name.endsWith("tgz") || jars.exists(name.contains(_))
  }

  val classpath = Attributed.data(cp).filter(wanted) ++ getExternalComponents.value
  str.log.info(s"files from classpath to copy are: ${classpath.map(_.getAbsolutePath).mkString("\n\t","\n\t","")}")
  val destinations = classpath.map(f => bd / "app" / "components" / f.getName)
  val filesToDelete = sbt.IO.listFiles(bd / "app" / "components").filterNot(f => destinations.exists(_.getAbsolutePath == f.getAbsolutePath))
  sbt.IO.delete(filesToDelete)
  sbt.IO.copy(classpath zip destinations)
  destinations
}

peScript := {
  val components = packageCMWell.value
  streams.value.log.info(s"components: ${components.map(_.getAbsolutePath).mkString("[",",","]")}")
  baseDirectory.value / "app" / "cmwell-pe.sh"
}

uploadInitContent := {
  baseDirectory.value / "app" / "upload-content.sh"
}

test in Test := {
  peScript.value
  (test in Test).value
}//.tag(CMWellTags.Grid)

fork in Test := true

baseDirectory in Test := file("cmwell-cons/app")

fullTest := {}
