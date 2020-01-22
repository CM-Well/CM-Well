/**
  * Copyright 2015 Thomson Reuters
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
package cmwell.util.log

import cmwell.util.string.sanitizeLogLine

import ch.qos.logback.classic.PatternLayout
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.pattern.PatternLayoutEncoderBase

class SanitizingPatternEncoder extends PatternLayoutEncoderBase[ILoggingEvent] {

  override def start(): Unit = {
    val patternLayout = new PatternLayout
    patternLayout.setContext(context)
    patternLayout.setPattern(getPattern)
    patternLayout.setOutputPatternAsHeader(outputPatternAsHeader)
    patternLayout.start()
    this.layout = patternLayout
    super.start()
  }

  private def convertToBytes(s: String) =
    if (super.getCharset == null) s.getBytes else s.getBytes(super.getCharset)

  override def encode(event: ILoggingEvent): Array[Byte] = {
    val txt = layout.doLayout(event)
    convertToBytes(sanitizeLogLine(txt))
  }
}