package awsutils
package aws

import awsutils.agg.ObjectsSize
import cats.{Comparison, Parallel}
import cats.effect.IO
import org.slf4j.Logger
import awsutils.lib._
import cats.kernel.Comparison.{EqualTo, GreaterThan, LessThan}
import cats.instances.lazyList._
import awsutils.io._

import scala.concurrent.duration._

object S3 {

  type S3RelativeKey = String
  type S3FileSize    = Long
  type S3Stats       = Map[S3RelativeKey, S3FileSize]

  /** Rewrites the keys of these statistics
    * '''Note:''' <code>f</code> has to be injective.
    */
  def rewriteKeys(f: S3RelativeKey => Option[S3RelativeKey])(stats: S3Stats): IO[S3Stats] = {
    import cats.syntax.traverse._

    def rewrite(s: S3RelativeKey): IO[S3RelativeKey] =
      IO(f(s)).flatMap {
        case Some(r) => IO.pure(r)
        case _       => IO.raiseError(new Exception(s"Rewriting path failed for ${s}"))
      }

    val keysBefore = stats.keys.size
    for {
      retStream <- stats.to(LazyList).traverse { case (k, v) => rewrite(k).map(_ -> v) }
      retMap    = retStream.toMap[S3RelativeKey, S3FileSize]
      keysAfter = retMap.keys.size
      _ <-
        if (keysAfter != keysBefore)
          IO.raiseError(new Exception(s"Rewriting S3Stats keys failed! (before=${keysBefore}, after=${keysAfter})."))
        else IO.pure(())
    } yield retMap
  }

  /** Collects all the name ans size of files in a table */
  def fileSizes(table: S3TableURL)(implicit logger: Logger, context: AwsAPI.Context): IO[S3Stats] = {
    val prefixSize = table.prefix.length
    for {
      _         <- IO(logger.info(s"Collecting file size for prefix ${table.url}"))
      fileSizes <- AwsAPI.objectStream(table).map(s => s.key().drop(prefixSize) -> s.size().toLong).compile.to(Map)
    } yield fileSizes
  }

  /** Compare two statistics about files and size. */
  def compareFileSizes(src: S3Stats, dst: S3Stats): Option[Comparison] = {
    import PartialOrdering._
    val longAsc = PartialOrdering.fromLT[Long]((x, y) => x < y).liftOption(NoneIs.Min)
    PartialOrdering.liftMap(longAsc).compare(src, dst)
  }

  /** Compare the files of two tables.
    * It supports rewriting relative path when performing key matching.
    */
  def computeFileSizeComparison(
      src: S3TableURL,
      dst: S3TableURL,
      srcPathRewriter: S3RelativeKey => Option[S3RelativeKey] = Some(_),
      dstPathRewriter: S3RelativeKey => Option[S3RelativeKey] = Some(_)
  )(implicit logger: Logger, context: AwsAPI.Context): IO[Option[Comparison]] = {
    import context.io.implicits._
    Parallel
      .parProduct(
        fileSizes(src).flatMap(rewriteKeys(srcPathRewriter)),
        fileSizes(dst).flatMap(rewriteKeys(dstPathRewriter))
      )
      .map { case (srcStats, dstStats) => compareFileSizes(srcStats, dstStats) }
  }

  def waitForS3(implicit ioc: IOContext): IO[Unit] = {
    import ioc.implicits._
    IO.sleep(1.minute)
  }

  /** A version a delete that verifies the table is empty at the end */
  def checkedDelete(table: S3TableURL)(implicit logger: Logger, aws: AwsAPI.Context): IO[Unit] = {
    val loggerUtils = new LoggerUtils(s => s"Deleting ${table.url}: $s.")
    import loggerUtils._
    import aws.impicits._
    info("Deleting table.")
    AwsAPI.deleteTable(table) *> waitForS3 *> AwsAPI.isEmpty(table).flatMap {
      case true  => info("Deletetion OK.")
      case false => error("Failed to delete table.", None)
    }
  }

  trait PathRewriter {
    def encode(s: String): Option[String]
    def decode(s: String): Option[String]
  }

  object PathRewriter {
    val idenrity: PathRewriter = new PathRewriter {
      def encode(s: String): Option[String] = Some(s)
      def decode(s: String): Option[String] = Some(s)
    }
  }

  /** A version of copy that verifies the two table are identical at the end */
  def checkedCopy(
      pr: PathRewriter
  )(src: S3TableURL, dst: S3TableURL)(implicit logger: Logger, aws: AwsAPI.Context): IO[Unit] = {
    val loggerUtils = new LoggerUtils(s => s"Copying ${src.url} to ${dst.url}: $s.")
    import loggerUtils._
    import aws.impicits._

    val checkFileSize: IO[Unit] =
      info("Checking.")
    computeFileSizeComparison(src, dst, Some(_), pr.decode).flatMap {
      case Some(EqualTo) => IO.pure(())
      case cmp           => error(s"Destination and source files do not match! (${cmp.toString}", None)
    }

    val copy: IO[Unit] =
      info(s"Copying.") *> AwsAPI.copyTable(src, dst, pr.encode) *> waitForS3 *> checkFileSize

    def cleanup(status: Either[Throwable, Unit]): IO[Unit] =
      status match {
        case Right(_) =>
          info(s"Copy OK.")
        case Left(err1) =>
          logError(s"Copying failed! Removing destination.", err1) *>
            checkedDelete(dst)
              .handleErrorWith { err2 =>
                err1.addSuppressed(err2)
                error("Cleanup deletion of destination failed!", Some(err1))
              }
              .flatMap(_ => IO.raiseError(err1))
      }

    info("Deleting destination before copying.") *>
      checkedDelete(dst) *>
      (copy.attempt.flatMap(cleanup))
  }

  /** Resumes a copy if possible, or run <code>onIncomparable</code> otherwise */
  def resumeCheckedCopy(pr: PathRewriter)(src: S3TableURL, dst: S3TableURL)(
      onIncomparable: => IO[Unit]
  )(implicit logger: Logger, aws: AwsAPI.Context): IO[Unit] =
    computeFileSizeComparison(src, dst, Some(_), pr.decode).flatMap {
      case Some(EqualTo) => IO.pure(())
      case Some(LessThan) =>
        IO.raiseError(new Exception(s"Copy source ${src.url} is lesser than destination ${dst.url}"))
      case Some(GreaterThan) => checkedCopy(pr)(src, dst)
      case _                 => onIncomparable
    }

  /** A version of move that verifies the two table are identical before deleting source. */
  def checkedMove(
      pr: PathRewriter
  )(src: S3TableURL, dst: S3TableURL)(implicit logger: Logger, aws: AwsAPI.Context): IO[Unit] =
    checkedCopy(pr)(src, dst) *> checkedDelete(src)

  /** Resumes a move if possible, or run <code>onIncomparable</code> otherwise */
  def resumeCheckedMove(pr: PathRewriter)(src: S3TableURL, dst: S3TableURL)(
      onIncomparable: => IO[Unit]
  )(implicit logger: Logger, aws: AwsAPI.Context): IO[Unit] =
    computeFileSizeComparison(src, dst, Some(_), pr.decode).flatMap {
      case Some(EqualTo)     => checkedDelete(src)
      case Some(LessThan)    => checkedDelete(src)
      case Some(GreaterThan) => checkedMove(pr)(src, dst)
      case _                 => onIncomparable
    }

  def objectsSize(src: S3TableURL)(implicit aws: AwsAPI.Context): IO[ObjectsSize] =
    AwsAPI
      .objectStream(src)
      .compile
      .fold(ObjectsSize.empty)((x, y) => ObjectsSize(x.objects + 1, x.size + y.size()))
}
