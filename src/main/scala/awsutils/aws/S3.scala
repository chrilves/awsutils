package awsutils
package aws

import cats.Comparison
import cats.effect.IO
import org.slf4j.Logger
import awsutils.lib._

object S3 {
  type S3Key      = String
  type S3FileSize = Long
  type S3Stats    = Map[S3Key, S3FileSize]

  def fileSizes(table: S3TableURL)(implicit logger: Logger, context: AwsAPI.Context): IO[S3Stats] =
    for {
      _         <- IO(logger.info(s"Collecting file size for prefix ${table.url}"))
      fileSizes <- AwsAPI.objectStream(table).map(s => s.key() -> s.size().toLong).compile.to(Map)
    } yield fileSizes

  def compareFileSizes(src: S3Stats, dst: S3Stats): Option[Comparison] = {
    import PartialOrdering._
    val longAsc = PartialOrdering.fromLT[Long]((x, y) => x < y).liftOption(NoneIs.Min)
    PartialOrdering.liftMap(longAsc).compare(src, dst)
  }

  def computeFileSizeComparison(src: S3TableURL, dst: S3TableURL)(implicit
      logger: Logger,
      context: AwsAPI.Context
  ): IO[Option[Comparison]] =
    for {
      srcStats <- fileSizes(src)
      dstStats <- fileSizes(dst)
    } yield compareFileSizes(srcStats, dstStats)
}
