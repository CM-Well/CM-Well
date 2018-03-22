package cmwell.util.collections.test

import cmwell.util.collections.spanWith
import org.scalatest.{FunSpec, Matchers}

/**
  * Proj: server
  * User: gilad
  * Date: 2/19/18
  * Time: 11:34 AM
  */
class SpanWithTests extends FunSpec with Matchers { //with OptionValues with EitherValues with TryValues with LazyLogging {
  describe("scanWith should") {
    it("span with mapping of the everything on the left side") {
      val (left, right) = spanWith(List(0, 2, 4, 6, 8, 7, 8, 6, 4, 2)) { i =>
        if (i % 2 == 0) Some(i / 2)
        else None
      }
      left shouldEqual List(0, 1, 2, 3, 4)
      right shouldEqual List(7, 8, 6, 4, 2)
    }

    it("span with mapping of everything") {
      val (left, right) = spanWith(List(0, 2, 4, 6, 8, 8, 6, 4, 2)) { i =>
        if (i % 2 == 0) Some(i / 2)
        else None
      }
      left shouldEqual List(0, 1, 2, 3, 4, 4, 3, 2, 1)
      right shouldEqual Nil
    }

    it("span with mapping of nothing") {
      val (left, right) = spanWith(List(1, 3, 5, 7, 9, 9, 7, 5, 3)) { i =>
        if (i % 2 == 0) Some(i / 2)
        else None
      }
      left shouldEqual Nil
      right shouldEqual List(1, 3, 5, 7, 9, 9, 7, 5, 3)
    }
  }
}
