package awsutils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import cats.Monoid
import awsutils.aws.AwsAPI.{AWSChunkSize, AWSParallelism, AWSRateLimit, AWSRetries}
import awsutils.aws.{AwsAPI, S3TableURL}
import awsutils.lib.CommandLineParser
import shapeless.tag
import awsutils.lib.CommandLineParser.{in, next, read}
import scala.concurrent.duration._

import scala.util.Try

sealed abstract class CommandLine extends Product with Serializable {
  val config: CommandLine.Config
}
object CommandLine {
  val usage: String =
    s"""AppEventsCompactor
       |
       |Usage:
       |  command (-h|--help)                       : prints this help message
       |  command CONFIG* copy   INPUT OUTPUT       : copy table to.
       |  command CONFIG* delete INPUT              : delete table.
       |  command CONFIG* monitor (INPUT CSV SECS)* : delete table.
       |
       |  INPUT  = s3://bucket/path/
       |  OUTPUT = s3://bucket/path/
       |  CSV    = /path/to/csv/file
       |  SECS   = integer representing seconds between two measures
       |
       |  CONFIG = (-acc |--aws-concurrent-calls)  <integer >  0>
       |         | (-ar  |--aws-retries)           <integer >  0>
       |         | (-acs |--aws-chunk-size)        <integer >  0> : size of delete requests
       |         | (-ap  |--aws-parallelism)       <integer >  0>
       |         | (-awoe|--aws-wait-on-error)     <integer >= 0> : seconds
       |
       |""".stripMargin

  val pS3TableURL: CommandLineParser[S3TableURL] =
    next.filterMap(S3TableURL.fromURL)

  val pInt: CommandLineParser[Int] =
    next.filterMap(x => Try(x.toInt))

  val pLong: CommandLineParser[Long] =
    next.filterMap(x => Try(x.toLong))

  val pDate: CommandLineParser[LocalDate] =
    read(s => LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE))

  /** Configuration Option */
  final case class Config(aws: AwsAPI.Config => AwsAPI.Config) {
    def +(c: Config): Config = Config(aws.andThen(c.aws))
  }
  object Config { self =>
    def empty: Config = Config(identity)

    implicit val configMonoid: Monoid[Config] =
      new Monoid[Config] {
        def empty: Config                         = self.empty
        def combine(x: Config, y: Config): Config = x + y
      }

    val parser: CommandLineParser[Config] = {
      val pAwsConcurentCalls =
        in(Set("-acc", "--aws-concurrent-calls"))
          .andr(pLong.filter(_ > 0L))
          .map(n => Config(_.copy(concurrentCalls = tag[AWSRateLimit](n))))

      val pAwsRetries =
        in(Set("-ar", "--aws-retries"))
          .andr(pInt.filter(_ > 0))
          .map(n => Config(_.copy(retries = tag[AWSRetries](n))))

      val pAwsChunkSize =
        in(Set("-acs", "--aws-chunk-size"))
          .andr(pInt.filter(_ > 0))
          .map(n => Config(_.copy(chunkSize = tag[AWSChunkSize](n))))

      val pAwsParallelism =
        in(Set("-ap", "--aws-parallelism"))
          .andr(pInt.filter(_ > 0))
          .map(n => Config(_.copy(parallelism = tag[AWSParallelism](n))))

      val pAwsWaitOnError =
        in(Set("-awoe", "--aws-wait-on-error"))
          .andr(pInt.filter(_ >= 0))
          .map { n => Config(_.copy(waitOnError = n.seconds)) }

      pAwsConcurentCalls
        .or(pAwsRetries)
        .or(pAwsChunkSize)
        .or(pAwsParallelism)
        .or(pAwsWaitOnError)
        .between(None, None)
        .map(_.foldLeft(Config.empty)(_ + _))
    }
  }

  final case object Help extends CommandLine {
    val config = Config.empty
  }
  final case class Copy(config: Config, src: S3TableURL, dst: S3TableURL) extends CommandLine
  final case class Delete(config: Config, src: S3TableURL)                extends CommandLine
  final case class Monitor(config: Config, items: List[Monitor.Item])     extends CommandLine
  final case class Error(error: Throwable) extends CommandLine {
    val config = Config.empty
  }

  object Monitor {
    final case class Item(url: S3TableURL, csv: String, waitBetween: FiniteDuration)

    object Item {
      val parse: CommandLineParser[Item] =
        for {
          url         <- pS3TableURL
          path        <- next
          waitBetween <- pLong
        } yield Item(url, path, waitBetween.seconds)
    }
  }

  val parser: CommandLineParser[CommandLine] = {
    import CommandLineParser._

    val pHelp: CommandLineParser[CommandLine] =
      in(Set("-h", "--help")).map(_ => Help)

    val pCopy: CommandLineParser[Config => CommandLine] =
      for {
        _   <- in(Set("copy"))
        src <- pS3TableURL
        dst <- pS3TableURL
      } yield Copy(_, src, dst)

    val pDelete: CommandLineParser[Config => CommandLine] =
      in(Set("delete")).andr(pS3TableURL).map(x => Delete(_, x))

    val pMonitor: CommandLineParser[Config => CommandLine] =
      in(Set("monitor")).andr(Monitor.Item.parse.between(None, None)).map(x => Monitor(_, x))

    pHelp
      .or {
        for {
          config <- Config.parser
          f      <- pCopy.or(pDelete).or(pMonitor)
        } yield f(config)
      }
      .andl(eof)
      .handleErrorWith(err => pure(Error(err)))
  }
}
