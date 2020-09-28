package awsutils.lib

import cats.effect.IO
import org.slf4j.Logger

/** Useful functions for logging */
final class LoggerUtils(context: String => String)(implicit logger: Logger) {
  def info(s: String): IO[Unit] =
    IO(logger.info(context(s)))

  def logError(s: String, e: Throwable): IO[Unit] =
    IO(logger.error(context(s), e))

  /** Raise an error and log it. */
  def error(s: String, e: Option[Throwable]): IO[Unit] = {
    val msg = context(s)
    val err = e.getOrElse(new Exception(msg))
    logError(msg, err) *> IO.raiseError(err)
  }

  /** If the condition is true, do nothing.
    * Fail with the provided error message otherwise */
  def check(cond: Boolean, msg: String): IO[Unit] =
    if (cond) IO.pure(()) else error(msg, None)
}
