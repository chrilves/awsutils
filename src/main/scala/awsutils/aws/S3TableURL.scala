package awsutils
package aws

import scala.util.{Failure, Success, Try}

final case class S3TableURL(bucket: String, path: List[String]) {
  def prefix: String            = path.map(_ + "/").mkString("")
  def prefixOpt: Option[String] = if (path.isEmpty) None else Some(prefix)
  def url: String               = s"s3://${bucket}/${prefix}"

  def append(subpath: String): S3TableURL = {
    val l = subpath.split("/+").filter(_.nonEmpty)
    S3TableURL(bucket, path ++ l)
  }
}
object S3TableURL {
  def fromURL(url: String): Try[S3TableURL] = {
    val tableRegex = "^s3(?:a|n)?://([^/]*)/*(.*)$".r
    url match {
      case tableRegex(b, p) =>
        Success(S3TableURL(b, p.split("/+").filter(_.nonEmpty).toList))
      case _ =>
        Failure(new Exception(s""""${url}" did not match "${tableRegex.toString}""""))
    }
  }

  implicit val s3TableURLOrdering: Ordering[S3TableURL] =
    Ordering.fromLessThan((x, y) => x.bucket < y.bucket || (x.bucket === y.bucket && x.prefix < y.prefix))
}
