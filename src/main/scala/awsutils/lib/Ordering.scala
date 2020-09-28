package awsutils
package lib

import cats.{Comparison, PartialOrder}
import cats.Comparison._
import cats.syntax.foldable._
import cats.instances.lazyList._

/** To see what a partial ordering is, please refer to
  * [[https://en.wikipedia.org/wiki/Partially_ordered_set#Formal_definition Partial Order]]*/
trait PartialOrdering[A] extends ((A, A) => Option[Comparison]) { self =>
  def compare(x: A, y: A): Option[Comparison]

  final def lessThan(x: A, y: A): Boolean     = compare(x, y) == Some(LessThan)
  final def greaterThan(x: A, y: A): Boolean  = compare(x, y) == Some(GreaterThan)
  final def equalTo(x: A, y: A): Boolean      = compare(x, y) == Some(EqualTo)
  final def incomparable(x: A, y: A): Boolean = compare(x, y) == None

  final def lessOrEq(x: A, y: A): Boolean =
    compare(x, y) match {
      case Some(LessThan) | Some(EqualTo) => true
      case _                              => false
    }

  final def greaterOrEq(x: A, y: A): Boolean =
    compare(x, y) match {
      case Some(GreaterThan) | Some(EqualTo) => true
      case _                                 => false
    }

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

  /** Lift this partial order over [[Option]].
    * <code>noneIs</code> indicates whether <code>None</code>
    * should be treated as the minimal/maximal value or
    * incomparable to <code>Some(a)</code>.
    */
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

  /** Indicates how to compare <None> to <code>Some(a)</code> in [[PartialOrdering.liftOption]] */
  sealed abstract class NoneIs
  object NoneIs {

    /** Indicates that <code>None < Some(a)</code> */
    case object Min extends NoneIs

    /** Indicates that <code>None > Some(a)</code> */
    case object Max extends NoneIs

    /** Indicates that <code>None</code> and <code>Some(a)</code>
      * can not be compared. */
    case object Incomparable extends NoneIs
  }

  def apply[A](ev: PartialOrdering[A]): ev.type = ev

  def from[A](f: (A, A) => Option[Comparison]): PartialOrdering[A] =
    new PartialOrdering[A] {
      def compare(x: A, y: A): Option[Comparison] = f(x, y)
    }

  /** Return the partial order corresponding to the function <code>f</code>
    * representing ''less than or equal to'':
    *
   * {{{ f(x,y) ⇔ x ≤ y }}}
    */
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

  /** Return the partial order corresponding to the function <code>f</code>
    * representing ''less than'':
    *
   * {{{ f(x,y) ⇔ x < y }}}
    *
   * Note: equality is standard equality [[==]].
    */
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

  /** Returns a [[PartialOrdering]] over <code>Map[A,B]</code> given a <code>PartialOrdering[B]</code>.
    *
   * Given <code>m1: Map[A,B]</code> and <code>m2: Map[A,B]</code>, then <code>m1 ≤ m2</code>
    * '''if and only if''' for all key <code>k</code> present in <code>m1</code> or <code>m2</code>:
    * <code>m1.get(k) ≤ m2.get(k)</code> using the <code>PartialOrdering[Option[B]]</code>.
    */
  implicit def liftMap[A, B](implicit B: PartialOrdering[Option[B]]): PartialOrdering[Map[A, B]] =
    new PartialOrdering[Map[A, B]] {
      def compare(x: Map[A, B], y: Map[A, B]): Option[Comparison] =
        (x.keySet ++ y.keySet)
          .to(LazyList)
          .foldMap((k: A) => B.compare(x.get(k), y.get(k)))(PartialComparisonBoundedSemilattice)
    }
}

/** Used in [[PartialOrdering.liftMap]] to compute how to map relate. */
object PartialComparisonBoundedSemilattice extends cats.kernel.BoundedSemilattice[Option[Comparison]] {

  /** Neutral element (left and right) of combine */
  def empty: Option[Comparison] = Some(EqualTo)

  /** idempotent, commutative, associative function */
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
