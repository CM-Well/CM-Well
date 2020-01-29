import java.io.File
import java.util.Locale

import CMWellCommon._

import coursier.cache.Cache
import coursier.util.Task
import sbt.Keys._
import sbt.librarymanagement.{Artifact, ModuleID}
import sbt.util.Logger
import sbt.{Keys, Project, SettingKey, Tags, TaskKey, _}
import sbtdynver.DynVerPlugin
import xerial.sbt.pack.PackPlugin

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object OSType extends Enumeration {
  type OSType = Value
  val Windows, MacOS, Linux, Other = Value
}

object CMWellBuild {

  type PartialFunction2[-T1, -T2, +R] = PartialFunction[Tuple2[T1, T2], R]
  lazy val apacheMirror = {
    val zoneID = java.util.TimeZone
      .getDefault()
      .getID
    if (zoneID.startsWith("America") || zoneID.startsWith("Pacific") || zoneID.startsWith("Etc")) "us"
    else "eu"
  }
  var detectedOS: OSType.Value = getOperatingSystemType

  def fetchZookeeper(version: String, buildFromSources: Option[(String, File => Future[File])] = None) = {

    import scala.concurrent.ExecutionContext.Implicits.global

    fetchZookeeperApacheMirror(version).recoverWith {
      case err1: Throwable => fetchZookeeperApacheArchive(version).recoverWith {
        case err2: Throwable => {
          buildFromSources.fold(combineThrowablesAsCauseAsync[File](err1, err2) { cause =>
            new Exception("was unable to fetch zookeeper binaries, and build from sources function isn't supplied", cause)
          }) {
            case (ext, build) => fetchZookeeperSourcesFromGithub(version, ext).flatMap(build)
          }
        }
      }
    }
  }

  import DynVerPlugin.autoImport._
  import autoImport._

  def fetchZookeeperApacheMirror(version: String)(implicit ec: ExecutionContext): Future[File] = {
    val ext = "tar.gz"
    val url = s"http://www-$apacheMirror.apache.org/dist/zookeeper/zookeeper-$version/apache-zookeeper-$version-bin.$ext"
    fetchArtifact(url)
  }

  def fetchZookeeperApacheArchive(version: String)(implicit ec: ExecutionContext): Future[File] = {
    val ext = "tar.gz"
    val url = s"https://archive.apache.org/dist/zookeeper/zookeeper-$version/apache-zookeeper-$version-bin.$ext"
    fetchArtifact(url)
  }

  def fetchZookeeperSourcesFromGithub(version: String, ext: String)(implicit ec: ExecutionContext): Future[File] = {
    require(ext == "zip" || ext == "tar.gz", s"invalid sources extension [$ext]")
    alternateUnvalidatedFetchArtifact(s"https://github.com/apache/zookeeper/archive/release-$version.$ext", ext)
  }

  def alternateUnvalidatedFetchArtifact(url: String, ext: String)(implicit es: ExecutionContext): Future[java.io.File] = {
    import coursier.util.Artifact

    val art = Artifact(
      url,
      Map.empty,
      Map.empty,
      changing = false,
      optional = false,
      None)

    coursier.cache.Cache.default.file(art).run.future.transform {
      case Success(Left(artifactError)) => Failure(new Exception(artifactError.message + ": " + artifactError.describe))
      case Success(Right(file)) => Success(file)
      case Failure(exception) => Failure(exception)
    }
  }

  def combineThrowablesAsCauseAsync[T](t1: Throwable, t2: Throwable)(f: Throwable => Throwable): Future[T] =
    Future.failed[T](combineThrowablesAsCause(t1, t2)(f))

  def combineThrowablesAsCause(t1: Throwable, t2: Throwable)(f: Throwable => Throwable): Throwable =
    f(Option(t1.getCause).fold(t1.initCause(t2)) { _ =>
      Option(t2.getCause).fold(t2.initCause(t1)) { _ =>
        t2
      }
    })

  def fetchKafka(scalaVersion: String, version: String, buildFromSources: Option[(String, File => Future[File])] = None) = {

    import scala.concurrent.ExecutionContext.Implicits.global

    fetchKafkaApacheMirror(scalaVersion, version).recoverWith {
      case err1: Throwable => fetchKafkaApacheArchive(scalaVersion, version).recoverWith {
        case err2: Throwable => {
          buildFromSources.fold(combineThrowablesAsCauseAsync[File](err1, err2) { cause =>
            new Exception("was unable to fetch kafka binaries, and build from sources function isn't supplied", cause)
          }) {
            case (ext, build) => fetchKafkaSourcesFromGithub(version, ext).flatMap(build)
          }
        }
      }
    }
  }

  def fetchKafkaApacheMirror(scalaVersion: String, version: String)(implicit ec: ExecutionContext): Future[File] = {
    val ext = "tgz"
    val url = s"http://www-$apacheMirror.apache.org/dist/kafka/$version/kafka_$scalaVersion-$version.$ext"
    fetchArtifact(url)
  }

  def fetchKafkaApacheArchive(scalaVersion: String, version: String)(implicit ec: ExecutionContext): Future[File] = {
    val ext = "tgz"
    val url = s"https://archive.apache.org/dist/kafka/$version/kafka_$scalaVersion-$version.$ext"
    fetchArtifact(url)
  }

  def fetchKafkaSourcesFromGithub(version: String, ext: String)(implicit ec: ExecutionContext): Future[File] = {
    require(ext == "zip" || ext == "tar.gz", s"invalid sources extension [$ext]")
    alternateUnvalidatedFetchArtifact(s"https://github.com/apache/kafka/archive/$version.$ext", ext)
  }

  def fetchCassandra(version: String, logger: Logger)(implicit ec: ExecutionContext): Future[(String, File)] = {
    val ext = "tar.gz"
    val fileName = s"apache-cassandra-$version-bin.$ext"
    fetchCassandraApacheMirror(version, fileName).recoverWith {
      case ex: Throwable =>
        logger.error(s"Fetching cassandra from the main mirror failed due to ${ex.getMessage} going to fetch it from apache archive.")
        fetchCassandraApacheArchive(version, fileName)
    }
      .map(fileName -> _)
  }

  def fetchCassandraApacheMirror(version: String, fileName: String)(implicit ec: ExecutionContext): Future[File] = {
    val url = s"http://www-$apacheMirror.apache.org/dist/cassandra/$version/$fileName"
    fetchArtifact(url)
  }

  def fetchArtifact(url: String)(implicit es: ExecutionContext): Future[java.io.File] = {
    import coursier.util.Artifact
    val sig = Artifact(
      url + ".asc",
      Map.empty,
      Map.empty,
      changing = false,
      optional = false,
      authentication = None
    )

    val art = Artifact(
      url,
      Map(
        "MD5" -> (url + ".md5"),
        "SHA-1" -> (url + ".sha1"),
        "SHA-256" -> (url + ".sha256"),
        "SHA-512" -> (url + ".sha512")
      ),
      Map("sig" -> sig),
      changing = false,
      optional = false,
      None)

    val checksums = Seq(Some("MD5"), Some("SHA-1"), Some("SHA-256"), Some("SHA-512"), None)
    val fileCache: Cache[Task] = coursier.cache.FileCache().withChecksums(checksums)
    fileCache.file(art).run.future.transform {
      case Success(Left(artifactError)) => Failure(new Exception(artifactError.message + ": " + artifactError.describe))
      case Success(Right(file)) => Success(file)
      case Failure(exception) => Failure(exception)
    }
  }

  def fetchCassandraApacheArchive(version: String, fileName: String)(implicit ec: ExecutionContext): Future[File] = {
    val url = s"https://archive.apache.org/dist/cassandra/$version/$fileName"
    fetchArtifact(url)
  }

  def fetchElasticSearch(version: String)(implicit ec: ExecutionContext): Future[(String, File)] = {
    val ext = "tar.gz"
    val osType = getOperatingSystemType match {
      case OSType.Linux => "linux"
      case OSType.MacOS => "darwin"
      case other => throw new Exception(s"Operating system $other is not supported")
    }
    val fileName = s"elasticsearch-oss-$version-$osType-x86_64.$ext"
    val url = s"https://artifacts.elastic.co/downloads/elasticsearch/$fileName"
    fetchArtifact(url).map(fileName -> _)
  }

  /**
    * detect the operating system from the os.name System property and cache
    * the result
    *
    * @returns - the operating system detected
    */
  def getOperatingSystemType: OSType.Value = {
    if (detectedOS == null) {
      val OS = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH)
      if (OS.contains("mac") || OS.contains("darwin")) detectedOS = OSType.MacOS
      else if (OS.contains("win")) detectedOS = OSType.Windows
      else if (OS.contains("nux")) detectedOS = OSType.Linux
      else detectedOS = OSType.Other
    }

    detectedOS
  }


  def projectSettings = Seq(
    (compile in Compile) := ((compile in Compile) dependsOn versionCheck).value,
    //scalafmtOnCompile := true,
    //doctestWithDependencies := false,
    Keys.fork in Test := true,
    libraryDependencies ++= {
      val dm = dependenciesManager.value
      Seq(
        dm("org.scalatest", "scalatest") % "test",
        dm("org.scalacheck", "scalacheck") % "test")
    },
    versionCheck := {
      val dynamicVersion = dynver.value
      val staticVersion = version.value
      if (dynamicVersion != staticVersion)
        sys.error(s"The version setting ($staticVersion) is different from the dynamic (dynver) one ($dynamicVersion). " +
          s"Please use the refreshVersion command to refresh the setting.")
    },
    testListeners := Seq(new sbt.JUnitXmlTestsListener(target.value.getAbsolutePath)),
    //		doctestTestFramework := DoctestTestFramework.ScalaTest,
    exportJars := true,
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " },
    fullTest := {},
    install in Compile := {
      fullTest.value
      packagedArtifacts.value
    },
    concurrentRestrictions in ThisBuild ++= Seq(
      Tags.limit(CommonTags.ES, 1),
      Tags.limit(CommonTags.Cassandra, 1),
      Tags.limit(CommonTags.Kafka, 1),
      Tags.limit(CommonTags.Grid, 1),
      Tags.exclusive(CommonTags.IntegrationTests)
    )
  )

  object autoImport {
    //  val configSettingsResource = TaskKey[Seq[sbt.File]]("config-settings-resource", "gets the .conf resource")
    val dependenciesManager = SettingKey[PartialFunction2[String, String, ModuleID]]("a setting containing versions for dependencies. if we only use it to declare " +
      "dependencies, we can avoid a lot of version collisions.")
    // scalastyle:off
    val iTestsLightMode = SettingKey[Boolean]("a flag, which if turns on, does not take cm-well down after integration tests, but rather purge everything in it, so it will be ready for next time. on startup, it checks if there is an instance running, and if so, does not re-install cm-well.")
    // scalastyle:on
    val peScript = TaskKey[sbt.File]("pe-script", "returns the script that executes cmwell in PE mode.")
    val uploadInitContent = TaskKey[sbt.File]("upload-init-content", "returns the script that uploads the initial content in to PE Cm-Well.")
    val packageCMWell = TaskKey[Seq[java.io.File]]("package-cmwell", "get components from dependencies and sibling projects.")
    val getLib = TaskKey[java.io.File]("get-lib", "creates a lib directory in cmwell-cons/app/ that has all cm-wll jars and their dependency jars.")
    val getCons = TaskKey[java.io.File]("get-cons", "get cons from cons project.")
    val getWs = TaskKey[java.io.File]("get-ws", "get ws from ws project.")
    val getTlog = TaskKey[java.io.File]("get-tlog", "get tlog tool tlog project.")
    val getBg = TaskKey[java.io.File]("get-bg", "get bg from batch project.")
    val getCtrl = TaskKey[java.io.File]("get-ctrl", "get ctrl from ctrl project.")
    val getDc = TaskKey[java.io.File]("get-dc", "get dc from dc project.")
    val getGremlin = TaskKey[java.io.File]("get-gremlin", "get gremlin plugin into cons.")
    val install = TaskKey[Map[Artifact, File]]("install", "build + test, much like 'mvn install'")
    val dataFolder = TaskKey[File]("data-folder", "returns the directory of static data to be uploaded")
    val printDate = TaskKey[Unit]("print-date", "prints the date")
    val fullTest = TaskKey[Unit]("fullTest", "executes all tests in project in parallel (with respect to dependent tests)")
    val getData = TaskKey[Seq[java.io.File]]("get-data", "get data to upload to cm-well")
    val getExternalComponents = TaskKey[Iterable[File]]("get-external-components", "get external dependencies binaries")
    val versionCheck = TaskKey[Unit]("test dyn version task")
  }
}
