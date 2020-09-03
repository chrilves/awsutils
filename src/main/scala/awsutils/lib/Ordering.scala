package awsutils
package lib

import cats.{Comparison, PartialOrder}
import cats.Comparison._
import cats.syntax.foldable._
import cats.instances.lazyList._

trait PartialOrdering[A] extends ((A, A) => Option[Comparison]) { self =>
  def compare(x: A, y: A): Option[Comparison]

  @inline final def apply(x: A, y: A): Option[Comparison] = compare(x, y)

  final def contraMap[B](f: B => A): PartialOrdering[B] =
    new PartialOrdering[B] {
      def compare(x: B, y: B): Option[Comparison] = self.compare(f(x), f(y))
    }

  def reverse: PartialOrdering[A] =
    new PartialOrdering[A] {
      def compare(x: A, y: A): Option[Comparison] =
        self.compare(x, y).map {
          case LessThan    => GreaterThan
          case GreaterThan => LessThan
          case _           => EqualTo
        }

      override def reverse: PartialOrdering[A] = self
    }

  def liftOption(noneIs: PartialOrdering.NoneIs): PartialOrdering[Option[A]] = {
    import PartialOrdering.NoneIs._
    val left = noneIs match {
      case Min          => Some(LessThan)
      case Max          => Some(GreaterThan)
      case Incomparable => None
    }

    val right = noneIs match {
      case Min          => Some(GreaterThan)
      case Max          => Some(LessThan)
      case Incomparable => None
    }

    new PartialOrdering[Option[A]] {
      def compare(x: Option[A], y: Option[A]): Option[Comparison] =
        (x, y) match {
          case (None, None)       => Some(EqualTo)
          case (Some(a), Some(b)) => self.compare(a, b)
          case (None, Some(_))    => left
          case (Some(_), None)    => right
        }
    }
  }
}

object PartialOrdering {
  sealed abstract class NoneIs
  object NoneIs {
    case object Min          extends NoneIs
    case object Max          extends NoneIs
    case object Incomparable extends NoneIs
  }

  def apply[A](ev: PartialOrdering[A]): ev.type = ev
  def from[A](f: (A, A) => Option[Comparison]): PartialOrdering[A] =
    new PartialOrdering[A] {
      def compare(x: A, y: A): Option[Comparison] = f(x, y)
    }

  def fromLE[A](f: (A, A) => Boolean): PartialOrdering[A] =
    new PartialOrdering[A] {
      def compare(x: A, y: A): Option[Comparison] =
        (f(x, y), f(y, x)) match {
          case (true, true) => Some(EqualTo)
          case (true, _)    => Some(LessThan)
          case (_, true)    => Some(GreaterThan)
          case (_, _)       => None
        }
    }

  def fromLT[A](f: (A, A) => Boolean): PartialOrdering[A] =
    new PartialOrdering[A] {
      def compare(x: A, y: A): Option[Comparison] =
        if (x == y)
          Some(EqualTo)
        else if (f(x, y))
          Some(LessThan)
        else if (f(y, x))
          Some(GreaterThan)
        else
          None
    }

  def fromCats[A](implicit p: PartialOrder[A]): PartialOrdering[A] =
    new PartialOrdering[A] {
      def compare(x: A, y: A): Option[Comparison] = p.partialComparison(x, y)
    }

  def fromOrdering[A](implicit A: Ordering[A]): PartialOrdering[A] =
    new PartialOrdering[A] {
      def compare(x: A, y: A): Option[Comparison] = {
        val r = A.compare(x, y)
        if (r == 0)
          Some(EqualTo)
        else if (r < 0)
          Some(LessThan)
        else
          Some(GreaterThan)
      }
    }

  implicit def liftMap[A, B](implicit B: PartialOrdering[Option[B]]): PartialOrdering[Map[A, B]] =
    new PartialOrdering[Map[A, B]] {
      def compare(x: Map[A, B], y: Map[A, B]): Option[Comparison] =
        (x.keySet ++ y.keySet)
          .to(LazyList)
          .foldMap((k: A) => B.compare(x.get(k), y.get(k)))(ParitalComparisonBoundedSemilattice)
    }
}

object ParitalComparisonBoundedSemilattice extends cats.kernel.BoundedSemilattice[Option[Comparison]] {
  def empty: Option[Comparison] = Some(EqualTo)

  def combine(xOpt: Option[Comparison], yOpt: Option[Comparison]): Option[Comparison] =
    for {
      x <- xOpt
      y <- yOpt
      c <- {
        if (x == EqualTo)
          Some(y)
        else if (y == EqualTo)
          Some(x)
        else if (x != y)
          None
        else
          Some(x)
      }
    } yield c
}
