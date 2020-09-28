package awsutils
package lib

import cats.{Monad, MonadError}
import cats.effect.IO

import scala.util.{Failure, Success, Try}

/** Tools to parse the command line (or any list of strings).
  * It is useful to build complex parsing strategies easily.
  *
 * Even if this is a case class, do not create objects with
  * the class constructor but use the provided API on the
  * companion object.
  * @tparam A the return type of the parser
  */
final case class CommandLineParser[A](value: (List[String]) => IO[(A, List[String])]) { self =>
  import CommandLineParser._

  /** Parse the list of string provided */
  def run(l: List[String]): IO[A] =
    value(l).map { case (a, _) => a }

  def map[B](f: A => B): CommandLineParser[B] =
    CommandLineParser((l0) => value(l0).map { case (a, l1) => (f(a), l1) })

  def mapError(f: Throwable => Throwable): CommandLineParser[A] =
    CommandLineParser((l0) => value(l0).handleErrorWith((e: Throwable) => IO.raiseError(f(e))))

  def flatMap[B](f: A => CommandLineParser[B]): CommandLineParser[B] =
    CommandLineParser((l0) => value(l0).flatMap { case (a, l1) => f(a).value(l1) })

  def handleErrorWith(f: Throwable => CommandLineParser[A]): CommandLineParser[A] =
    CommandLineParser((l) => value(l).handleErrorWith((e: Throwable) => f(e).value(l)))

  /** Makes state change local. When this parser ends, the input list is
    * restored to what it was when it began. */
  def local: CommandLineParser[A] =
    CommandLineParser((l) => value(l).map { case (a, _) => (a, l) })

  def attempt: CommandLineParser[Either[Throwable, A]] =
    CommandLineParser((l0) =>
      value(l0).attempt.map {
        case Left(e)        => (Left(e), l0)
        case Right((a, l1)) => (Right(a), l1)
      }
    )

  /** The dual of [[attempt]]
    * If this parser returns an <code>Either[Throwable, B]</code>.
    * Then returning <code>Left(err)</code> is turned into an error.
    */
  def reflect[B](implicit ev: A <:< Either[Throwable, B]): CommandLineParser[B] =
    CommandLineParser((l0) =>
      value(l0).flatMap {
        case (a, l1) =>
          ev(a) match {
            case Left(e)  => IO.raiseError(e)
            case Right(b) => IO.pure((b, l1))
          }
      }
    )

  def filter(p: A => Boolean): CommandLineParser[A] =
    filterMap((a) => if (p(a)) Success(a) else Failure(new Exception(s"Filetr predicate failed}")))

  def filterMap[B](p: A => Try[B]): CommandLineParser[B] =
    flatMap { a =>
      p(a) match {
        case Failure(e) => raiseError(e)
        case Success(b) => pure(b)
      }
    }

  def ap[C, D](arg: CommandLineParser[C])(implicit ev: A <:< (C => D)): CommandLineParser[D] =
    flatMap((f) => arg.map(f))

  /** Runs this parser and then <code>other</code>, returns both values. */
  def and[B](other: CommandLineParser[B]): CommandLineParser[(A, B)] =
    flatMap((a) => other.map((b) => (a, b)))

  /** Runs this parser and then <code>other</code>, returns only this parser's value. */
  def andl[B](other: CommandLineParser[B]): CommandLineParser[A] =
    flatMap((a) => other.map((_) => (a)))

  /** Runs this parser and then <code>other</code>, returns only <code>other</code> parser's value.*/
  def andr[B](other: CommandLineParser[B]): CommandLineParser[B] =
    flatMap(_ => other)

  /** If this parser fails, then try <code>other</code>. */
  def or(other: CommandLineParser[A]): CommandLineParser[A] =
    handleErrorWith(_ => other)

  /** Run this parser as many times as possible between <code>from</code> and <code>to</code> times.
    * Return the list of parsed values.
    *
   * - If <code>to = Some(n)</code>, then the parser reads no more than <code>n</code> items.
    * - If <code>from = Some(n)</code>, then the parser fails if it was not able to parse
    *   at least <code>n</code> values.
    */
  def between(from: Option[Int], to: Option[Int]): CommandLineParser[List[A]] =
    tailRecM[List[A], List[A]](Nil) { (acc) =>
      val n = acc.size
      if (to.exists(_ <= n))
        pure(Right(acc.reverse))
      else
        flatMap[Either[List[A], List[A]]](a => pure(Left(a :: acc)))
          .handleErrorWith { error =>
            if (from.forall(_ <= n))
              pure(Right(acc.reverse))
            else {
              val msg = s"Failed at step ${n} afer ${acc.reverse
                .mkString(",")} (from ${from.map(_.toString).getOrElse("")} to ${to.map(_.toString).getOrElse("")}"
              raiseError(new Exception(msg, error))
            }
          }
    }
}
object CommandLineParser { self =>

  /** Turn the lazy value into a Parser that returns this value. */
  def apply[A](value: => A): CommandLineParser[A] =
    CommandLineParser((l: List[String]) => IO((value, l)))

  def liftIO[A](io: IO[A]): CommandLineParser[A] =
    CommandLineParser((l) => io.map(a => (a, l)))

  /** Turn the strict value into a Parser that returns this value. */
  def pure[A](a: A): CommandLineParser[A] =
    CommandLineParser((l) => IO.pure((a, l)))

  /** Succeeds if the input list is empty, fails otherwise */
  def eof: CommandLineParser[Unit] =
    CommandLineParser((l) => if (l.isEmpty) IO.pure(((), l)) else IO.raiseError(new Exception("Not empty")))

  /** Consumes and returns the next argument from the input list. */
  def next: CommandLineParser[String] =
    CommandLineParser((l) =>
      l match {
        case hd :: tl => IO.pure((hd, tl))
        case _        => IO.raiseError(new Exception("Empty"))
      }
    )

  /** A parser that fails with the error provided. */
  def raiseError[A](e: Throwable): CommandLineParser[A] =
    CommandLineParser(_ => IO.raiseError(e))

  /** Runs <code>f</code> as long as it returns [[Left]].
    * Avoid stack overflow when looping.
    *
   * @param a0 the initial value to feed <code>f</code>.
    * @param f the function to loop on.
    * @return the value <code>b</code> when <code>f</code> returns <code>Right(b)</code>.
    */
  def tailRecM[A, B](a0: A)(f: A => CommandLineParser[Either[A, B]]): CommandLineParser[B] =
    CommandLineParser { (l0) =>
      Monad[IO].tailRecM((a0, l0)) {
        case (a1, l1) =>
          f(a1).value(l1).map {
            case (Left(a2), l2) => Left((a2, l2))
            case (Right(b), l2) => Right((b, l2))
          }
      }
    }

  /** Transform the next argument into an <code>A</code>. */
  def read[A](f: String => A): CommandLineParser[A] =
    next.map(f)

  /** Reads the next argument in the input list.
    * - Succeeds if it is contained in <code>idents</code>.
    * - Fail otherwise.
    *
   * The membership test is performed up to <code>norm</code>.
    * It is useful to support case insensitive matching.
    *
   * Returns the argument read.
    */
  def in(idents: Set[String], norm: String => String = identity[String]): CommandLineParser[String] = {
    val normMap = idents.map(s => norm(s) -> s).toMap
    next.flatMap { s =>
      normMap.get(norm(s)) match {
        case Some(i) => pure(i)
        case _       => raiseError(new Exception(s""""${s}" not in ${idents.mkString(",")}"""))
      }
    }
  }

  /** The usual parsing of "-short" and "--long" */
  def option(short: Char, long: String): CommandLineParser[String] =
    in(Set(s"-${short}", s"--${long}"))

  implicit val commandLineParserInstances0: MonadError[CommandLineParser, Throwable] =
    new MonadError[CommandLineParser, Throwable] {
      def pure[A](x: A): CommandLineParser[A] =
        self.pure(x)
      def raiseError[A](e: Throwable): CommandLineParser[A] =
        self.raiseError(e)
      def flatMap[A, B](fa: CommandLineParser[A])(f: A => CommandLineParser[B]): CommandLineParser[B] =
        fa.flatMap(f)
      def handleErrorWith[A](fa: CommandLineParser[A])(f: Throwable => CommandLineParser[A]): CommandLineParser[A] =
        fa.handleErrorWith(f)
      def tailRecM[A, B](a: A)(f: A => CommandLineParser[Either[A, B]]): CommandLineParser[B] =
        self.tailRecM(a)(f)
    }
}
