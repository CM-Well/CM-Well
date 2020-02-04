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
import com.typesafe.scalalogging.LazyLogging
import scala.annotation.tailrec
import scala.concurrent._ //,ExecutionContext.Implicits.global
import scala.concurrent.duration.{Deadline, Duration, FiniteDuration, TimeUnit}
import scala.util.{Failure, Success, Try}
import scala.language.postfixOps
import scala.language.implicitConversions

/**
  * Created with IntelliJ IDEA.
  * User: gilad
  * Date: 7/23/13
  * Time: 10:49 AM
  * To change this template use File | Settings | File Templates.
  */
package cmwell.util {

  import scala.collection.BuildFrom

  package object concurrent extends LazyLogging {
    //TODO: return "Thread++" instead of thread..

    def startOnThread(body: => Unit): Thread = {
      onThread(true, body)
    }

    def delayOnThread(body: => Unit): Thread = {
      onThread(false, body)
    }

    private def onThread(runNow: Boolean, body: => Unit): Thread = {
      val t = new Thread {
        override def run =
          body //TODO: throw exception if got exception. (contains a vector of all the exception we got)
      }
      if (runNow) {
        t.start
      }
      t
    }

    def delay(deadline: Deadline): Unit = {
      Try(Await.ready(Promise().future, deadline.timeLeft))
    }

    /**
      * receives delay and task that returns type A to execute.
      * the task will be executed after the delay and the future will be completed with the task's return value.
      *
      * @param delay
      * @param task
      * @param ec
      * @tparam A
      * @return
      */
    def delayedTask[A](delay: FiniteDuration)(task: => A)(implicit ec: ExecutionContext): Future[A] = {
      val p = Promise[A]()
      SimpleScheduler.schedule(delay) {
        p.complete(Try(task))
      }
      p.future
    }

    /**
      *
      * @param future
      * @param duration timeout duration
      * @param ec
      * @tparam T
      * @return new future that either return in time with the original value,
      *         or fail with a FutureTimeout exception that holds the original future
      */
    def timeoutFuture[T](future: Future[T], duration: FiniteDuration)(implicit ec: ExecutionContext): Future[T] = {
      val p = Promise[T]()
      p.tryCompleteWith(future)

      val (_,c) = SimpleScheduler.scheduleInstant(duration) { p.tryFailure(FutureTimeout(future)) }
      future.andThen { case _ => c.cancel() }

      p.future
    }

    /**
      *
      * @param future
      * @param duration
      * @param ec
      * @tparam T
      * @return new future that fails if the original future fails,
      *         or Option filled with either the original future's value,
      *         or None if future did not complete in time.
      */
    def timeoutOptionFuture[T](future: Future[T],
                               duration: FiniteDuration)(implicit ec: ExecutionContext): Future[Option[T]] = {
      timeoutFuture(future, duration)(ec)
        .map(Some.apply)
        .recover {
          case FutureTimeout(_) => None
        }
    }

    implicit def asFiniteDuration(d: java.time.Duration) =
      scala.concurrent.duration.Duration.fromNanos(d.toNanos)

    /**
      * @param maxRetries max numbers to retry the task
      * @param delay "cool-down" wait period
      * @param task the task to run
      */
    def retry[T](maxRetries: Int, delay: Duration = Duration.Zero, delayFactor: Double = 0)(
      task: => Future[T]
    )(implicit ec: ExecutionContext): Future[T] = {
      require(maxRetries > 0, "maxRetries must be positive")
      require(delay >= Duration.Zero, "delay must be non-negative")
      if (maxRetries == 1) task
      else
        task.recoverWith {
          case _: Throwable => {
            if (delay == Duration.Zero) retry(maxRetries - 1)(task)
            else {
              val nextDelay = if (delayFactor > 0) delay * delayFactor else delay
              SimpleScheduler.scheduleFuture(delay)(retry(maxRetries - 1, nextDelay, delayFactor)(task))
            }
          }
        }
    }

    /**
      * @param maxRetries max numbers to retry the task
      * @param pf a function to compute "wait duration" from the retry number and the type of the last failed attempt
      * @param task the task to run
      * @return
      */
    def retryWithPF[T](maxRetries: Int, pf: PartialFunction[(Int, Throwable), FiniteDuration])(
      task: => Future[T]
    )(implicit ec: ExecutionContext): Future[T] = {
      require(maxRetries > 0, "maxRetries must be positive")

      def retryInner(retryNum: Int): Future[T] = {
        if (maxRetries == retryNum) task
        else
          task.recoverWith {
            case t: Throwable =>
              val d = pf(retryNum -> t)
              if (d <= Duration.Zero) retryInner(maxRetries + 1)
              else SimpleScheduler.scheduleFuture(d)(retryInner(maxRetries + 1))
          }
      }
      retryInner(1)
    }

    def retryWithDelays[T](delays: FiniteDuration*)(task: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      if (delays.isEmpty) task
      else
        task.recoverWith {
          case _: Throwable =>
            val (delay, tail) = (delays.head, delays.tail)
            if (delay == Duration.Zero) retryWithDelays(tail: _*)(task)
            else SimpleScheduler.scheduleFuture(delay)(retryWithDelays(tail: _*)(task))
        }
    }

//    def betterRetry[T](maxRetries: Int, waitBetweenRetries: Option[FiniteDuration] = None)
//                      (task: => Future[T])(implicit ec: ExecutionContext, sys: ActorSystem): Future[T] = {
//      require(maxRetries > 0, "maxRetries must be positive")
//      if (maxRetries > 1) task.recoverWith {
//        case _: Throwable => waitBetweenRetries.fold(betterRetry(maxRetries - 1)(task)) { waitTime =>
//          val p = Promise[T]
//          sys.scheduler.scheduleOnce(waitTime) {
//            p.completeWith(betterRetry(maxRetries - 1, waitBetweenRetries)(task))
//          }
//          p.future
//        }
//      } else task
//    }

    def safeFuture[T](fun: => Future[T]): Future[T] = {
      Try[Future[T]] { fun }.recover { case t: Throwable => Future.failed(t) }.get
    }

    /**
      * @param maxRetries max numbers to retry the task
      * @param isSuccessful a predicate to determine if the task was successful or not
      * @param delay "cool-down" wait period
      * @param task the task to run
      *
      * this retry flavor works with a predicate instead of a failed future
      */
    def unsafeRetryUntil[T](isSuccessful: T => Boolean,
                            maxRetries: Int,
                            delay: FiniteDuration = Duration.Zero,
                            delayFactor: Long = 0)(task: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      require(maxRetries > 0, "maxRetries must be positive")
      require(delay >= Duration.Zero, "delay must be non-negative")
      if (maxRetries == 1) task
      else
        task.flatMap {
          case t if isSuccessful(t)        => Future.successful(t)
          case t if delay == Duration.Zero => unsafeRetryUntil(isSuccessful, maxRetries - 1)(task)
          case t =>
            val nextDelay = if (delayFactor > 0) delay * delayFactor else delay
            SimpleScheduler.scheduleFuture(delay)(
              unsafeRetryUntil(isSuccessful, maxRetries - 1, nextDelay, delayFactor)(task)
            )
        }
    }

//    def retryUntil2[T, S](z: S)(shouldRetry: (Try[T], S) => ShouldRetry[S])(task: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
//      def scheduleRetry(delay: FiniteDuration, newState: S): Future[T] = {
//        if (delay == Duration.Zero) retryUntil2(newState)(shouldRetry)(task)
//        else SimpleScheduler.scheduleFuture(delay)(retryUntil2(newState)(shouldRetry)(task))
//      }
//
//      val p = Promise[T]()
//      task.onComplete { t =>
//        shouldRetry(t, z) match {
//          case DoNotRetry => p.complete(t)
//          case RetryWith(delay, newState) => p.completeWith(scheduleRetry(delay, newState))
//        }
//      }
//      p.future
//    }

    def retryUntil[T, S: StateHandler](
      z: S
    )(shouldRetry: (Try[T], S) => ShouldRetry[S])(task: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val p = Promise[T]()
      task.onComplete { taskResult =>
        shouldRetry(taskResult, z) match {
          case DoNotRetry => {
            p.complete(taskResult)
          }
          case RetryWith(newState) =>
            p.completeWith(retryUntil(newState)(shouldRetry)(implicitly[StateHandler[S]].handle[T](newState)(task)))
        }
      }
      p.future
    }

//    def retryUntil[T](shouldRetry: Try[T] => Boolean, maxRetries: Int, delay: FiniteDuration = Duration.Zero, delayFactor: Long = 1)
//                     (task: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
//      require(maxRetries > 0, "maxRetries must be positive")
//      require(delay >= Duration.Zero, "delay must be non-negative")
//      def scheduleRetry: Future[T] = {
//        if (delay == Duration.Zero) retryUntil(shouldRetry, maxRetries - 1)(task)
//        else SimpleScheduler.scheduleFuture(delay)(retryUntil(shouldRetry, maxRetries - 1, delay * delayFactor, delayFactor)(task))
//      }
//      if (maxRetries == 1) task
//      else task.flatMap { t =>
//        if (shouldRetry(Success(t))) scheduleRetry else Future.successful(t)
//      }.recoverWith {
//        case err if shouldRetry(Failure[T](err)) => scheduleRetry
//        case err => Future.failed(err)
//      }
//    }
//
    /**
      * Does what Future.sequence does but it won't fail if one of the futures fails.
      * Instead, the failed element won't be included in the produced collection.
      *
      * @param in Collection of futures that we want to turn into future of collection.
      * @param bf CanBuildFrom
      * @param ec Execution context.
      * @return Future of the given collection.
      */
    def successes[A, M[X] <: Iterable[X]](in: M[Future[A]])(implicit bf: BuildFrom[M[Future[A]], A, M[A]],
                                                               ec: ExecutionContext): Future[M[A]] = {
      in.foldLeft(Future.successful(bf(in))) { (fr, fa) =>
          {
            fa.flatMap(a => fr.map(_ += a)(ec))(ec).recoverWith { case _: Throwable => fr }(ec)
          }
        }
        .map(_.result())
    }

    object Successfulness {
      implicit val bolleanToSuccessfulness: Boolean => Successfulness = {
        case true  => Successful
        case false => Recoverable
      }
    }
    sealed trait Successfulness
    case object Successful extends Successfulness
    case object Recoverable extends Successfulness
    case object UnRecoverable extends Successfulness

    val spinCheckTimeout = {
      import scala.concurrent.duration.DurationInt
      30.seconds
    }

    def spinCheck[T](
      interval: FiniteDuration,
      returnOriginalElementOnFailure: Boolean = false,
      maxTimeUntilGivingUp: FiniteDuration = spinCheckTimeout
    )(task: => Future[T])(isSuccessful: T => Successfulness): Future[T] = {

      import SimpleScheduler.{scheduleFuture => sf}

      val startTime = System.currentTimeMillis()

      def wait(): Future[T] = {
        task.flatMap { elem =>
          val timeSpent = System.currentTimeMillis() - startTime

          Try(isSuccessful(elem)) match {
            case Success(Recoverable) if timeSpent < maxTimeUntilGivingUp.toMillis => sf(interval)(wait())
            case Success(Successful)                                               => Future.successful(elem)
            case Success(_) if returnOriginalElementOnFailure                      => Future.successful(elem)
            case Success(_)                                                        => Future.failed(new IllegalStateException(s"got a bad element: $elem"))
            case Failure(exception) =>
              logger.error(s"got exception for element: $elem", exception)
              if (timeSpent < maxTimeUntilGivingUp.toMillis) sf(interval)(wait())
              else Future.failed(new IllegalStateException(s"got a bad element: $elem", exception))
          }
        }(ExecutionContext.Implicits.global)
      }
      wait()
    }

    def executeAfterCompletion[T](f: Future[_])(body: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val p = Promise[T]()
      f.onComplete(_ => p.completeWith(try { body } catch { case err: Throwable => Future.failed[T](err) }))
      p.future
    }

    /**
      * Transforms a `TraversableOnce[A]` into a `Future[Vector[B]]` using the provided function `A => Future[B]`.
      * This is useful for performing a parallel map. For example, to apply a function to all items of a list
      * in parallel:
      *
      * {{{
      *   val myFutureVector = travector(myTraversable)(x => Future(myFunc(x)))
      * }}}
      *
      * This is the same as `Future.traverse`, but it will always yield a vector, regardless of the provided collection type
      */
    def travector[A, B, M[X] <: TraversableOnce[X]](
      in: M[A]
    )(fn: A => Future[B])(implicit executor: ExecutionContext): Future[Vector[B]] =
      in.foldLeft(Future.successful(Vector.newBuilder[B])) { (fr, a) =>
          val fb = fn(a)
          for (r <- fr; b <- fb) yield (r += b)
        }
        .map(_.result())

    /**
      * Transforms a `TraversableOnce[A]` into a `Future[List[B]]` using the provided function `A => Future[B]`.
      * This is useful for performing a parallel map. For example, to apply a function to all items of a list
      * in parallel:
      *
      * {{{
      *   val myFutureList = travelist(myTraversable)(x => Future(myFunc(x)))
      * }}}
      *
      * This is the same as `Future.traverse`, but it will always yield a List, regardless of the provided collection type
      */
    def travelist[A, B, M[X] <: TraversableOnce[X]](
      in: M[A]
    )(fn: A => Future[B])(implicit executor: ExecutionContext): Future[List[B]] =
      in.foldLeft(Future.successful(List.newBuilder[B])) { (fr, a) =>
          val fb = fn(a)
          for (r <- fr; b <- fb) yield (r += b)
        }
        .map(_.result())

    /**
      * Transforms a `TraversableOnce[A]` into a `Future[Set[B]]` using the provided function `A => Future[B]`.
      * This is useful for performing a parallel map. For example, to apply a function to all items of a list
      * in parallel:
      *
      * {{{
      *   val myFutureList = travelist(myTraversable)(x => Future(myFunc(x)))
      * }}}
      *
      * This is the same as `Future.traverse`, but it will always yield a Set, regardless of the provided collection type
      */
    def travset[A, B, M[X] <: TraversableOnce[X]](
      in: M[A]
    )(fn: A => Future[B])(implicit executor: ExecutionContext): Future[Set[B]] =
      in.foldLeft(Future.successful(Set.newBuilder[B])) { (fr, a) =>
          val fb = fn(a)
          for (r <- fr; b <- fb) yield r.+=(b)
        }
        .map(_.result())

    /**
      * similar to [[travector]] with the addition of optional accumulation
      */
    def collector[A, B, M[X] <: IterableOnce[X]](
      in: M[A]
    )(fn: A => Future[Option[B]])(implicit executor: ExecutionContext): Future[Vector[B]] =
      in.iterator.foldLeft(Future.successful(Vector.newBuilder[B])) { (fr, a) =>
          val fb = fn(a)
          for (r <- fr; ob <- fb) yield ob.fold(r)(r.+=)
        }
        .map(_.result())

    /** Transforms a `TraversableOnce[A]` into a `Future[Map[K,B]]` using the provided function `A => Future[(K,V)]`.
      *  This is useful for performing a parallel map. For example, to apply a function to all items of a list
      *  in parallel:
      *
      *  {{{
      *    val myFutureVector = travemp(myTraversable)(x => Future(myFunc(x)))
      *  }}}
      *
      * This is similar to `Future.traverse`, but it will always yield a Map, regardless of the provided collection type
      */
    def travemp[A, K, V, M[X] <: TraversableOnce[X]](
      in: M[A]
    )(fn: A => Future[(K, V)])(implicit executor: ExecutionContext): Future[Map[K, V]] =
      in.foldLeft(Future.successful(Map.newBuilder[K, V])) { (fr, a) =>
          val fb = fn(a)
          for (r <- fr; b <- fb) yield (r += b)
        }
        .map(_.result())

    // http://stackoverflow.com/questions/28277843/convert-scala-future-to-java-future
    // this is not 1:1 conversion, since `.cancel` is unsupported

    import java.util.concurrent.{Future => JFuture}

    def asJava[A](fut: Future[A]): JFuture[A] = {
      new JFuture[A] {
        override def get(): A = Await.result(fut, Duration.Inf)

        override def get(timeout: Long, unit: TimeUnit): A = Await.result(fut, Duration.create(timeout, unit))

        override def isDone: Boolean = fut.isCompleted

        override def cancel(mayInterruptIfRunning: Boolean): Boolean = throw new UnsupportedOperationException

        override def isCancelled: Boolean = false
      }
    }
  }

  package concurrent {
    case class FutureTimeout[T](future: scala.concurrent.Future[T]) extends Exception

    trait StateHandler[S] {
      def handle[T](state: S)(task: => Future[T]): Future[T]
    }

    object StateHandler {
      implicit val durationHandler = new StateHandler[FiniteDuration] {
        override def handle[T](delay: FiniteDuration)(task: => Future[T]): Future[T] = {
          if (delay == Duration.Zero) task
          else SimpleScheduler.scheduleFuture(delay)(task)
        }
      }

      implicit val retryParamsHandler = new StateHandler[RetryParams] {
        override def handle[T](state: RetryParams)(task: => Future[T]) = {
          if (state.delay == Duration.Zero) task
          else SimpleScheduler.scheduleFuture(state.delay)(task)
        }
      }
    }

    case class RetryParams(retriesLeft: Int, delay: FiniteDuration, delayFactor: Double)

    sealed trait ShouldRetry[+S]
    case object DoNotRetry extends ShouldRetry[Nothing]
    final case class RetryWith[S](state: S) extends ShouldRetry[S]
  }

}
