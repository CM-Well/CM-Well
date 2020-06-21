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
package cmwell.util

import java.util.zip.{ZipEntry, ZipFile}
import scala.collection.JavaConverters._

package object loading {
  def extractClassNamesFromJarFile(jarFilePath: String): Seq[String] = {
    def isNamedClass(entry: ZipEntry): Boolean = !entry.isDirectory && !entry.getName.contains('$')
    def normalize(fileLikeClassname: String): String = fileLikeClassname.replaceAll("/", ".").replaceAll(".class", "")

    new ZipFile(jarFilePath).entries().asScala.filter(isNamedClass).map(ent => normalize(ent.getName)).toSeq
  }
}
