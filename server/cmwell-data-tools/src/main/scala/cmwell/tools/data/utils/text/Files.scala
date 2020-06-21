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
package cmwell.tools.data.utils.text

import java.text.DecimalFormat

object Files {
  private[this] val B = 1L
  private[this] val K = 1024L
  private[this] val M = K * K
  private[this] val G = M * K
  private[this] val T = G * K

  def toHumanReadable(value: Double): String = toHumanReadable(value.toLong)

  def toHumanReadable(value: Long): String = {
    if (value < 0) {
      throw new IllegalArgumentException("Invalid file size: " + value);
    }

    value match {
      case v if v < 0  => throw new IllegalArgumentException("Invalid file size: " + value)
      case v if v == 0 => "0B"
      case v if v < K  => format(v, B, "B")
      case v if v < M  => format(v, K, "KB")
      case v if v < G  => format(v, M, "MB")
      case v if v < T  => format(v, G, "GB")
      case v           => format(v, T, "TB")
    }
  }

  def format(value: Long, divider: Long, unit: String) = {
    val result = if (divider >= 0) value * 1.0 / divider else value * 1.0
    new DecimalFormat(s"#,##0.##$unit").format(result)
  }
}
