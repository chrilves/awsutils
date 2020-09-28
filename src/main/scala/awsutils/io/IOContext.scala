package awsutils
package io

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.duration.FiniteDuration

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

  /** Retry this IO <code>n</code> times. */
  def retry[A](n: Int, waitDuration: FiniteDuration)(io: IO[A])(implicit ioc: IOContext): IO[A] =
    if (n <= 0)
      io
    else {
      io.handleErrorWith { err1 =>
        import ioc.implicits._

        IO(err1.printStackTrace()) *>
          IO.sleep(waitDuration) *>
          retry(n - 1, waitDuration)(io).handleErrorWith { err2 =>
            IO.raiseError {
              err2.addSuppressed(err1)
              err2
            }
          }
      }
    }
}
