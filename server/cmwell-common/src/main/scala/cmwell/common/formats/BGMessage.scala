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

// scalastyle:off
sealed abstract class Offset(val topic: String,
                             val offset: Long,
                             val part: Int,
                             val ofParts: Int) extends Comparable[Offset] {

  override def compareTo(o: Offset) = {
    if (o != null) offset.compareTo(o.offset)
    else throw new NullPointerException("Comparing null values is not supported!")
  }

  override def hashCode(): Int = offset.hashCode()

  // These are not "globally" true but rather suitable for our use when handling done offsets
  override def equals(obj: scala.Any): Boolean = {
    obj != null                  &&
    obj.isInstanceOf[Offset]     &&
    obj.asInstanceOf[Offset].offset == offset
  }
}
// scalastyle:on

case class CompleteOffset(override val topic: String, override val offset: Long)
    extends Offset(topic, offset, 1, 1)
case class PartialOffset(override val topic: String,
                         override val offset: Long,
                         override val part: Int,
                         override val ofParts: Int)
    extends Offset(topic, offset, part, ofParts)

case class BGMessage[T](offsets: Seq[Offset] = Seq.empty[Offset], message: T)

object BGMessage {
  def apply[T](offset: Offset, message: T): BGMessage[T] =
    new BGMessage(Seq(offset), message)
  def apply[T](message: T) = new BGMessage(message = message)
}
