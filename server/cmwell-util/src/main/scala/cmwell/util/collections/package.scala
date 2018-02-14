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


package cmwell.util

import java.io.{IOException, InputStream}

import com.google.common.cache.LoadingCache

import scala.annotation.tailrec
import scala.concurrent._
import scala.language.{higherKinds, postfixOps}
import scala.collection.{GenTraversable, SeqLike, TraversableLike, mutable}
import scala.collection.generic.{CanBuildFrom, GenericCompanion}
import scala.collection.immutable.Set
import scala.collection.mutable.{Set => MSet}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * Created by michael on 10/28/15.
 */
package object collections {

  def filterByIndices[T, Coll[_]]
    (xs: Coll[T])
    (p: Int => Boolean)
    (implicit ev: Coll[T] <:< SeqLike[T,Coll[T]], cbf: CanBuildFrom[Coll[T], T, Coll[T]]): Coll[T] = {

    xs.foldLeft(cbf(xs) -> 0) {
      case ((b,i),e) =>
        val j = i + 1
        if(p(i)) b.+=(e) -> j
        else b -> j
    }._1.result()
  }

  def partition3[T, Coll[_]]
    (xs: Coll[T])
    (f: T => Int)
    (implicit ev: Coll[T] <:< TraversableLike[T,Coll[T]], cbf: CanBuildFrom[Coll[T], T, Coll[T]]): (Coll[T],Coll[T],Coll[T]) = {

    val b0 = cbf(xs)
    val b1 = cbf(xs)
    val b2 = cbf(xs)

    for(x <- xs) f(x) match {
      case 0 => b0 += x
      case 1 => b1 += x
      case 2 => b2 += x
      case n => throw new IllegalStateException(s"f($x) returned $n which is not in range of 0..2 and was not expected")
    }

    (b0.result(), b1.result(), b2.result())
  }


  def mapFirst[A,B,Coll[_]]
    (xs: Coll[A])
    (f: A => Option[B])
    (implicit ev: Coll[A] <:< TraversableLike[A,Coll[A]]): Option[B] = {
    var ob: Option[B] = None
    xs.find { a =>
      ob = f(a)
      ob.isDefined
    }
    ob
  }

  def updatedMultiMap[K,V](m: Map[K,List[V]], k: K, v: V): Map[K,List[V]] =
    m.updated(k, v :: m.getOrElse(k,Nil))

  def subtractedMultiMap[K,V](m: Map[K,List[V]], k: K, v: V): Map[K,List[V]] =
    m.get(k).fold(m){
      case Nil => m - k
      case `v` :: Nil => m - k
      case many => m.updated(k,many.filterNot(v.==))
    }

  def updatedDistinctMultiMap[K,V](m: Map[K,Set[V]], k: K, v: V): Map[K,Set[V]] =
    m.updated(k, m.getOrElse(k,Set.empty) + v)

  def subtractedDistinctMultiMap[K,V](m: Map[K,Set[V]], k: K, v: V): Map[K,Set[V]] =
    m.get(k).fold(m){ set =>
      val sub = set - v
      if(sub.isEmpty) m - k
      else m.updated(k,sub)
    }

  /**
   * `partition` and `map` combined.
   * for a given collection, and a function from the collection elements to `Either[A,B]`,
   * generates a tuple of 2 collections of types `A` and `B`
   *
   * @param xs the collection of elements
   * @param f a function that convert an element to an `Either[A,B]`
   * @tparam A left collection elements' type
   * @tparam B right collection elements' type
   * @tparam T original collection elements' type
   * @tparam Coll collection's type
   */
  def partitionWith[A, B, T, Coll[_]]
  (xs: Coll[T])
  (f: T => Either[A,B])
  (implicit ev: Coll[T] <:< TraversableLike[T,Coll[T]],
   cbf1: CanBuildFrom[Coll[T], A, Coll[A]],
   cbf2: CanBuildFrom[Coll[T], B, Coll[B]]): (Coll[A],Coll[B]) = {

    val b1 = cbf1(xs)
    val b2 = cbf2(xs)

    for(x <- xs) f(x) match {
      case Left(a) => b1 += a
      case Right(b) => b2 += b
    }

    b1.result() -> b2.result()
  }

  /**
   * like `distinct`, but uses a function to generate "distinctiveness"
   *
   * @param xs given collection
   * @param f a function to distinct elements by it's output
   * @tparam T collection type
   * @tparam R distinct by type
   * @tparam Coll type of the collection
   */
  def distinctBy[T, R, Coll[_]]
  (xs: Coll[T])
  (f: T => R)
  (implicit ev: Coll[T] <:< TraversableLike[T, Coll[T]],
   cbf: CanBuildFrom[Coll[T], T, Coll[T]]): Coll[T] = {

    val builder = cbf(xs)
    builder.sizeHint(xs.size)
    val seen = MSet.empty[R]

    xs.foreach { elem =>
      if (!seen(f(elem))) {
        builder += elem
        seen.add(f(elem))
      }
    }
    builder.result()
  }

  /**
   * reverse flatten. result collection type is of the inner collection.
   * e.g:
   *
   * {{{
   * scala> import cmwell.util.collections._
   * import cmwell.util.collections._
   *
   * scala> val nested = List(Set(1,2),Set(2,3))
   * nested: List[scala.collection.immutable.Set[Int]] = List(Set(1, 2), Set(2, 3))
   *
   * scala> nested.flatten
   * res0: List[Int] = List(1, 2, 2, 3)
   *
   * scala> innerFlat(nested)
   * res1: scala.collection.immutable.Set[Int] = Set(1, 2, 3)
   * }}}
   *
   * @param xs input
   * @tparam T elements type
   * @tparam Inner inner collection type
   * @tparam Outer outer collection type
   */
  def innerFlat[T,Inner,Outer[_]]
                (xs: Outer[Inner])
                (implicit ev0: Inner <:< TraversableLike[T, Inner],
                 ev1: Outer[Inner] <:< TraversableLike[Inner, Outer[Inner]],
                 cbf: CanBuildFrom[Inner,T,Inner]): Inner = {

    val size: Int = (0 /: xs)((ac,in)=>ac+in.size)
    val builder = cbf()
    builder.sizeHint(size)

    xs.foreach(builder ++= _)
    builder.result()
  }

  def optry[T](ot: Option[Try[T]]): Try[Option[T]] = ot.fold(Success(None): Try[Option[T]]){
    case s: Success[T] => s.map(Option.apply[T])
    case f: Failure[T] => f.asInstanceOf[Try[Option[T]]]
  }

  def tryop[T](to: Try[Option[T]]): Option[Try[T]] = to match {
    case Success(o) => o.map(Success.apply)
    case f: Failure[_] => Some(f.asInstanceOf[Try[T]])
  }

  def opfut[T](of: Option[Future[T]])(implicit ec: ExecutionContext): Future[Option[T]] =
    of.fold(Future.successful(Option.empty[T]))(_.map(Some.apply))

  // http://stackoverflow.com/a/4905770/4244787
  //  Stream.continually(is.read).takeWhile(-1!=).map(_.toByte).toArray
  def readInputStreamToByteArray(is: InputStream): Array[Byte] = {
    val b = Array.newBuilder[Byte]
    var i = is.read
    while(i != -1) {
      b += i.toByte
      i = is.read
    }
    b.result()
  }

  def randomFrom[Elem,Coll]
                (xs: Coll)
                (implicit ev: Coll <:< TraversableLike[Elem, Coll]): Elem = {
    xs.toIterator
      .drop(scala.util.Random.nextInt(xs.size))
      .next()
  }

  trait BytesConverter[T] {
    def asBytes(t: T): Array[Byte]
  }

  object BytesConverter {
    implicit object StringConverter extends BytesConverter[String] {
      def asBytes(s: String): Array[Byte] = s.getBytes("UTF-8")
    }
  }

  /**
   * NOT THREAD SAFE!
   * will convert an iterator for some type that can be converted to bytes,
   * into a plain old `InputStream`
   *
   * @param f iterator generation function
   * @tparam T a type that can be converted to bytes
   * @return InputStream
   */
  def asInputStream[T : BytesConverter](f: () => Iterator[T]): InputStream = new InputStream {
    val converter = implicitly[BytesConverter[T]]
    val it = f()
    var arr: Array[Byte] = Array.empty[Byte]
    var pos = 0

    @tailrec
    override def read(): Int = {

      def wrapUnsafeAndConvertToIOException[X](x: => X) = {
        try { x }
        catch {
          case ioe: IOException => throw ioe
          case t: Throwable => throw new IOException(t)
        }
      }

      def updateAndGet() = {
        val rv = arr(pos).toInt & 255
        pos += 1
        if (arr.length == pos) {
          arr = Array.empty
          pos = 0
        }
        rv
      }

      if (arr.nonEmpty) wrapUnsafeAndConvertToIOException(updateAndGet())
      else {
        val hasNext = wrapUnsafeAndConvertToIOException(it.hasNext)
        if (hasNext) {
          wrapUnsafeAndConvertToIOException(arr = converter.asBytes(it.next()))
          read()
        }
        else -1
      }
    }
  }

  /**
   * Unfolder can be used to explicitly unfold to some collection type.
   * e.g:
   *
   * {{{
   * scala> import cmwell.util.collections._
   * import cmwell.util.collections._
   *
   * scala> Unfolder.unfold(Array.newBuilder[Int])(0 → 1){
   *      |   case (a,b) if a > 1000 ⇒ None
   *      |   case (a,b) ⇒ Some((b→(a+b))→a)
   *      | }
   * res0: Array[Int] = Array(0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987)
   * }}}
   *
   * note this will also work for user defined collections,
   * if you'll explicitly supply a builder for it.
   */
  object Unfolder {
    def unfold[CC[_], E, S](b: mutable.Builder[E, CC[E]])(z: S)(f: S => Option[(S, E)]): CC[E] = {
      var s = f(z)
      while(s.isDefined) {
        val Some((state,element)) = s
        b += element
        s = f(state)
      }
      b.result()
    }
  }

  /**
   * generic implicit unfolder for any collection that has a `GenericCompanion`,
   * e.g: `Set`,`Vector`,`List`,`Traversable`,etc'...
   * can be used with the companion object like:
   *
   * {{{
   * scala> import cmwell.util.collections._
   * import cmwell.util.collections._
   *
   * scala> Set.unfold(0 → 1){
   *      |   case (a,b) if a > 1000 ⇒ None
   *      |   case (a,b) ⇒ Some((b→(a+b))→a)
   *      | }
   * res0: scala.collection.immutable.Set[Int] = Set(0, 5, 89, 1, 233, 21, 610, 13, 2, 34, 144, 377, 3, 55, 8, 987)
   *
   * scala> Traversable.unfold(1){ i ⇒
   *      |   if(i > 10) None
   *      |   else Some((i+1,i*i))
   *      | }
   * res1: Traversable[Int] = List(1, 4, 9, 16, 25, 36, 49, 64, 81, 100)
   * }}}
   */
  implicit class Unfolder[CC[X] <: GenTraversable[X]](obj: GenericCompanion[CC]) {
    def unfold[S,E](z: S)(f: S => Option[(S, E)]): CC[E] =
      Unfolder.unfold(obj.newBuilder[E])(z)(f)
  }

  /**
   * `Array` does not have a `GenericCompanion`, so it has a special implementation.
   *
   * {{{
   * scala> import cmwell.util.collections._
   * import cmwell.util.collections._
   *
   * scala> Array .unfold(1){ i ⇒
   *      |   if(i > 10) None
   *      |   else Some((i+1,i*i))
   *      | }
   * res0: Array[Int] = Array(1, 4, 9, 16, 25, 36, 49, 64, 81, 100)
   * }}}
   */
  implicit class ArrayUnfolder(arrObj: Array.type) {
    def unfold[S,E : ClassTag](z: S)(f: S => Option[(S,E)]): Array[E] =
      Unfolder.unfold(Array.newBuilder[E])(z)(f)
  }

  /**
   * `Map` is generic with 2 types: `K` and `V`, so it also gets to have it's own implementation.
   *
   * {{{
   * scala> import cmwell.util.collections._
   * import cmwell.util.collections._
   *
   * scala> Map.unfold(1){ i ⇒
   *      |   if(i > 10) None
   *      |   else Some((i+1,(i,i*i)))
   *      | }
   * res0: Map[Int,Int] = Map(5 -> 25, 10 -> 100, 1 -> 1, 6 -> 36, 9 -> 81, 2 -> 4, 7 -> 49, 3 -> 9, 8 -> 64, 4 -> 16)
   * }}}
   */
  implicit class MapUnfolder(mapObj: Map.type) {
    def unfold[S,K,V](z: S)(f: S => Option[(S,(K,V))]): Map[K,V] = {
      val b = Map.newBuilder[K, V]
      var s = f(z)
      while(s.isDefined) {
        val Some((state,element)) = s
        b += element
        s = f(state)
      }
      b.result()
    }
  }

  /**
   * `Stream` gets it's own implementation, because although it inherits from `GenericCompanion`,
   * it is more appropriate to preserve it's lazy nature, and not compute the entire stream eagerly.
   * this allows for potentially infinite sequences:
   *
   * {{{
   * scala> import cmwell.util.collections._
   * import cmwell.util.collections._
   *
   * scala> val s = Stream.unfold(1){ i => Some((i << 1, i.toString))}
   * s: Stream[String] = Stream(1, ?)
   *
   * scala> s(10)
   * res0: String = 1024
   *
   * scala> s
   * res1: Stream[String] = Stream(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, ?)
   * }}}
   */
  implicit class StreamUnfolder(strObj: scala.collection.immutable.Stream.type) {
    def unfold[S,E](z: S)(f: S => Option[(S,E)]): Stream[E] = {
      f(z).fold(Stream.empty[E]) {
        case (s, e) => e #:: unfold(s)(f)
      }
    }
  }

  /**
   * `Iterator` gets it's own implementation, because:
   * 1). it doesn't inherit from `GenericCompanion`.
   * 2). it should only unfold itself upon `next()` calls.
   * this allows for potentially infinite sequences:
   *
   * {{{
   * scala> import cmwell.util.collections._
   * import cmwell.util.collections._
   *
   * scala> val i = Iterator.unfold(1){ i => Some((i << 1, i.toString))}
   * i: Iterator[String] = non-empty iterator
   *
   * scala> i.take(10).toList
   * res0: List[String] = List(1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
   * }}}
   */
  implicit class IteratorUnfolder(itrObj: scala.collection.Iterator.type) {
    def unfold[S,E](z: S)(f: S => Option[(S,E)]): Iterator[E] = new Iterator[E] {
      private[this] var nxt = f(z)
      override def hasNext: Boolean = nxt.isDefined
      override def next(): E = {
        val (s,e) = nxt.get
        nxt = f(s)
        e
      }
    }
  }

  implicit class TryOps(tryObj: scala.util.Try.type) {
    def sequence[A, M[X] <: TraversableOnce[X]](in: M[Try[A]])(implicit bf: CanBuildFrom[M[Try[A]], A, M[A]]): Try[M[A]] = {
      in.foldLeft(Success(bf(in)): Try[mutable.Builder[A,M[A]]]) {
        (tr, ta) => tr.flatMap { b =>
          ta.map(b.+=)
        }
      }.map(_.result())
    }

    def traverse[A, B, M[X] <: TraversableOnce[X]](in: M[A])(f: A => Try[B])(implicit bf: CanBuildFrom[M[A], B, M[B]]): Try[M[B]] = {
      in.foldLeft(Success(bf(in)): Try[mutable.Builder[B,M[B]]]) {
        (tr, a) => tr.flatMap { b =>
          f(a).map(b.+= )
        }
      }.map(_.result())
    }
  }

  implicit class OptionOps(optObj: scala.Option.type) {
    def sequence[A, M[X] <: TraversableOnce[X]](in: M[Option[A]])(implicit bf: CanBuildFrom[M[Option[A]], A, M[A]]): Option[M[A]] = {
      in.foldLeft(Some(bf(in)): Option[mutable.Builder[A,M[A]]]) {
        (or,oa) => or.flatMap { b =>
          oa.map(b.+=)
        }
      }.map(_.result())
    }

    def traverse[A, B, M[X] <: TraversableOnce[X]](in: M[A])(f: A => Option[B])(implicit bf: CanBuildFrom[M[A], B, M[B]]): Option[M[B]] = {
      in.foldLeft(Some(bf(in)): Option[mutable.Builder[B,M[B]]]) {
        (or, a) => or.flatMap { b =>
          f(a).map(b.+= )
        }
      }.map(_.result())
    }
  }

  implicit class LoadingCacheExtensions[K,V](lc: LoadingCache[K,V]) {
    def getAsync(key: K)(implicit ec: ExecutionContext): Future[V] = Option(lc.getIfPresent(key)).fold(Future(blocking(lc.get(key))))(Future.successful)
    def getBlocking(key: K): V = Option(lc.getIfPresent(key)).getOrElse(blocking(lc.get(key)))
  }
}