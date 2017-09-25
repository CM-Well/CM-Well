package cmwell.util.stream

import akka.stream.{Inlet, Outlet, Shape}

import scala.collection.immutable

/**
  * Proj: server
  * User: gilad
  * Date: 9/5/17
  * Time: 6:02 AM
  */
case class SingleTypeAmorphousShape[I,O](inlets: immutable.Seq[Inlet[I]], outlets: immutable.Seq[Outlet[O]]) extends Shape {
  override def deepCopy() = SingleTypeAmorphousShape(inlets.map(_.carbonCopy()), outlets.map(_.carbonCopy()))
}
