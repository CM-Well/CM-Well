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


package cmwell.it.fixture

trait NSHashesAndPrefixes {
  lazy val (metaNsPaths,ns) = {
    val nsRegex = """(<>\s<cmwell://meta/ns#)(.+)(>\s<)(.+)(>\s\.)""".r
    val nsResource = Thread.currentThread.getContextClassLoader.getResourceAsStream("meta_ns_prefixes_snapshot_infotons.nt")
    val xs = scala.io.Source.fromInputStream(nsResource).getLines()
    val b = Set.newBuilder[String]
    var rdf: String = ""
    var vcard: String = ""
    var rel: String = ""
    var dc: String = ""
    xs.foreach {
      case nsRegex(start, prefix, middle, uri, end) => {
        val hash = cmwell.util.string.Hash.crc32base64(uri)
        b += s"/meta/ns/$hash"
        if      (prefix == "rdf")   rdf   = hash
        else if (prefix == "vcard") vcard = hash
        else if (prefix == "rel")   rel   = hash
        else if (prefix == "dc")    dc    = hash
      }
    }
    b.result() -> NSHashes(rdf,vcard,rel,dc)
  }
}

case class NSHashes(rdf: String, vcard: String, rel: String, dc: String)
