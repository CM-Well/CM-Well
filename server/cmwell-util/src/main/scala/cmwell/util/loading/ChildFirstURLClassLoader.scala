/**
  * © 2019 Refinitiv. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package cmwell.util.loading

import java.io.{File, InputStream}
import java.net.{URL, URLClassLoader}

// scalastyle:off
import sun.misc.CompoundEnumeration
// scalastyle:on
import scala.util.{Failure, Success, Try}

/**
  * Created by yaakov on 7/20/15.
  * based on http://stackoverflow.com/a/6424879/4244787
  *
  * DEFAULT LOADING ORDER: SELF, PARENT
  * But, when loading class that except.exists(className.startsWith), ignoring self.
  *
  * Not using System's ClassLoader explicitly
  *
  */
class ChildFirstURLClassLoader(urls: Array[URL], parent: ClassLoader, except: Seq[String] = Seq())
    extends URLClassLoader(urls, parent) {

  protected override def loadClass(name: String, resolve: Boolean): Class[_] = {
    def tryFind(findAction: => Class[_]): Option[Class[_]] = Try(findAction) match {
      case Failure(e: ClassNotFoundException) => None
      case Failure(e)                         => throw e
      case Success(c)                         => Some(c)
    }

    def loadLocally = if (except.exists(name.startsWith)) None else tryFind(findClass(name))
    def loadFromParent = if (getParent == null) None else tryFind(getParent.loadClass(name))

    val alreadyLoaded = findLoadedClass(name)
    if (alreadyLoaded != null) {
      alreadyLoaded
    } else {

      val `class` = loadLocally.getOrElse(loadFromParent.orNull)

      if (resolve)
        resolveClass(`class`)
      `class`
    }
  }

  override def getResource(name: String): URL = findResource(name) match {
    case null => super.getResource(name)
    case u    => u
  }

  override def getResources(name: String): java.util.Enumeration[URL] = {
    val parent = getParent
    val localUrls = findResources(name)
    val parentUrls: java.util.Enumeration[URL] =
      if (parent != null) parent.getResources(name) else java.util.Collections.emptyEnumeration()
    new CompoundEnumeration(Array(localUrls, parentUrls))
  }

  override def getResourceAsStream(name: String): InputStream = {
    getResource(name) match {
      case null => null
      case url =>
        Try(url.openStream) match {
          case Success(x) => x
          case Failure(_) => null
        }
    }
  }
}

object ChildFirstURLClassLoader {
  def loadClassFromJar[T](className: String, jarPath: String, commonPackageNames:String, excludes: Seq[String] = Seq()): T =
    Loader(jarPath, excludes :+ commonPackageNames).load(className)

  case class Loader(jarPath: String, excludes: Seq[String] = Seq()) {
    val urls = if(new java.io.File(jarPath).isFile) Array(new File(jarPath).toURI.toURL) else Array[URL](new URL(jarPath))
    private val cl =
      new ChildFirstURLClassLoader(urls, this.getClass.getClassLoader, excludes)
    def load[T](className: String) = cl.loadClass(className).newInstance.asInstanceOf[T]
  }

}
