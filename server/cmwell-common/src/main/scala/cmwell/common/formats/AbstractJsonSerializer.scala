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
package cmwell.common.formats

import com.fasterxml.jackson.core.JsonFactory
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTimeZone
import org.joda.time.format.ISODateTimeFormat

/**
  * Created by gilad on 2/26/15.
  */
object SettingsHelper {
  val config = ConfigFactory.load()
  val dataCenter = config.getString("dataCenter.id")
}

class AbstractJsonSerializer {

  val jsonFactory = new JsonFactory()
  val dateFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC)
}

trait NsSplitter {
  def splitNamespaceField(field: String) = field.lastIndexOf(".") match {
    case -1 => "nn" -> field
    case i  => field.substring(i + 1) -> field.substring(0, i).replace('.', '_')
  }

  def reverseNsTypedField(field: String) = {
    if (field == "_all") "allFields"
    else if (field.startsWith("system.") || field.startsWith("content.") || field.startsWith("link.")) field
    else {
      val (ns, typedField) = splitNamespaceField(field)
      "fields." + ns + "." + typedField.replace('.', '_')
    }
  }
}
