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

/**
  * the following is taken (with enhancements) from:
  * https://github.com/nummulus/boite
  *
  * (it is unmaintained and not published against new scala versions)
  *
  */
sealed abstract class Box[+A] {
  self =>

  /**
    * Returns {@code true} if the box contains no value (Empty or Failure),
    * {@code false} otherwise.
    */
  def isEmpty: Boolean

  /**
    * Returns {@code true} if the box is Failure,
    * {@code false} otherwise.
    */
  def isFailure: Boolean = false

  /**
    * Returns {@code true} if the box contains a value, {@code false} otherwise.
    */
  def isDefined: Boolean = !isEmpty

  /**
    * Returns the value of the box.
    *
    * @throws NoSuchElementException if the box is empty, or original exception if the box is failure
    */
  def get: A

  /**
    * Returns the exception if this is failure.
    *
    * @throws NoSuchElementException if the box is empty, or full
    */
  def getFailure: Throwable = throw new NoSuchElementException("Not a BoxedFailure")

  /**
    * Returns the value of the box if it's full, else the specified default.
    */
  def getOrElse[B >: A](default: => B): B

  /**
    * Applies a function to the value of the box if it's full and returns a
    * new box containing the result. Returns empty otherwise.
    * <p>
    * Differs from flatMap in that the given function is not expected to wrap
    * the result in a box.
    *
    * @see flatMap
    */
  def map[B](f: A => B): Box[B] = EmptyBox

  /**
    * Applies a function to the value of the box if it's full and returns a
    * new box containing the result. Returns empty otherwise.
    * <p>
    * Differs from map in that the given function is expected to return a box.
    *
    * @see map
    */
  def flatMap[B](f: A => Box[B]): Box[B] = EmptyBox

  /**
    * Applies a function to the value of the box if it's full, otherwise do
    * nothing.
    */
  def foreach[U](f: A => U) {}

  /**
    * Returns a List of one element if the box is full or an empty list
    * otherwise.
    */
  def toList: List[A] = List.empty[A]

  /**
    * Returns an Option. `Some` if the box is full or `None` otherwise.
    */
  def toOption: Option[A] = if (isEmpty) None else Some(this.get)

  /**
    * Returns {@code true} if both objects are equal based on the contents of
    * the box. For failures, equality is based on equivalence of failure
    * causes.
    */
  override def equals(other: Any): Boolean = (this, other) match {
    case (FullBox(x), FullBox(y)) => x == y
    case (x, y: AnyRef)           => x eq y
    case _                        => false
  }

  override def hashCode: Int = this match {
    case FullBox(x) => x.##
    case _          => super.hashCode
  }
}

object Box {

  import scala.language.implicitConversions

  /**
    * Implicitly converts a Box to an Iterable.
    * This is needed, for instance to be able to flatten a List[Box[_]].
    */
  implicit def box2Iterable[A](b: Box[A]): Iterable[A] = b.toList

  /**
    * A Box factory which converts a scala.Option to a Box.
    */
  def apply[A](o: Option[A]): Box[A] = o match {
    case Some(value) => FullBox(value)
    case None        => EmptyBox
  }

  /**
    * A Box factory which returns a Full(f) if f is not null, Empty if it is,
    * and a Failure if f throws an exception.
    */
  def wrap[A](f: => A): Box[A] =
    try {
      val value = f
      if (value == null) EmptyBox else FullBox(value)
    } catch {
      case e: Exception => BoxedFailure(e)
    }

  def empty[A]: Box[A] = EmptyBox
}

final case class FullBox[+A](value: A) extends Box[A] {
  override def isEmpty = false

  override def get: A = value

  override def getOrElse[B >: A](default: => B): B = value

  override def map[B](f: A => B): Box[B] = FullBox(f(value))

  override def flatMap[B](f: A => Box[B]): Box[B] = f(value)

  override def foreach[U](f: A => U) { f(value) }

  override def toList: List[A] = List(value)
}

private[util] sealed abstract class BoxWithNothing extends Box[Nothing] {
  override def isEmpty = true

  override def getOrElse[B >: Nothing](default: => B): B = default
}

case object EmptyBox extends BoxWithNothing {
  override def get: Nothing = throw new NoSuchElementException("Box does not contain a value")
}

final case class BoxedFailure(exception: Throwable) extends BoxWithNothing {
  type A = Nothing

  override def get: Nothing = throw exception

  override def getFailure: Throwable = exception

  override def isFailure: Boolean = true

  override def map[B](f: A => B): Box[B] = this

  override def flatMap[B](f: A => Box[B]): Box[B] = this

  override final def equals(other: Any): Boolean = (this, other) match {
    case (BoxedFailure(x), BoxedFailure(a)) => (x) == (a)
    case _                                  => false
  }

  override final def hashCode: Int = exception.##
}

object BoxedFailure {
  def apply(message: String) = new BoxedFailure(new Exception(message))
}
