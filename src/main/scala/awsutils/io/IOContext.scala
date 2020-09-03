package awsutils
package io

import cats.effect.{ContextShift, IO, Timer}

final case class IOContext(
    contextShift: ContextShift[IO],
    timer: Timer[IO]
) {
  object implicits {
    implicit val ioContextContextShift: ContextShift[IO] = contextShift
    implicit val ioContextTimerAWSContext: Timer[IO]     = timer
  }
}
object IOContext {
  def create(implicit ec: scala.concurrent.ExecutionContext): IOContext =
    IOContext(IO.contextShift(ec), IO.timer(ec))

  def retry[A](n: Int)(io: IO[A]): IO[A] =
    if (n <= 0)
      io
    else
      io.handleErrorWith { err1 =>
        IO(err1.printStackTrace()).flatMap { _ =>
          retry(n - 1)(io).handleErrorWith { err2 =>
            IO.raiseError {
              err2.addSuppressed(err1)
              err2
            }
          }
        }
      }
}
