package awsutils

import cats.effect.IO
import shapeless.tag
import software.amazon.awssdk.regions.Region
import awsutils.aws._
import scala.concurrent.ExecutionContext
import awsutils.aws.AwsAPI._
import awsutils.io.IOContext

object Main {
  val defaultAwsConfig: AwsAPI.Config =
    AwsAPI.Config(
      Region.EU_WEST_1,
      tag[AWSRateLimit](10L),
      tag[AWSRetries](3),
      tag[AWSChunkSize](200),
      tag[AWSParallelism](20)
    )

  /** Resources needed for the application */
  final case class Context(io: IOContext, aws: AwsAPI.Context) {
    object implicits {
      implicit val compactorContextIOContext: IOContext       = io
      implicit val compactorContextAwsContext: AwsAPI.Context = aws
    }
  }

  object Context {

    /** Initialize the resources needed for the application */
    def create(awsConfig: AwsAPI.Config)(implicit ec: ExecutionContext): IO[Context] = {
      implicit val io: IOContext = IOContext.create
      for {
        aws <- AwsAPI.Context.create(awsConfig)
        _ <- IO(println {
          s"""aws-concurrent-calls=${awsConfig.concurrentCalls}
             |aws-reties=${awsConfig.retries}
             |aws-chunk-size=${awsConfig.chunkSize}
             |aws-parallelism=${awsConfig.parallelism}
             |""".stripMargin
        })
      } yield Context(io, aws)
    }
  }

  def main(args: Array[String]): Unit = {
    (for {
      command <- CommandLine.parser.run(args.toList)
      awsConfig = command.config.aws(defaultAwsConfig)
      context <- Context.create(awsConfig)(ExecutionContext.global)
      r <- {
        import CommandLine._
        import context.implicits._
        command match {
          case Help =>
            IO(println(s"${Console.GREEN}${usage}${Console.RESET}"))
          case Error(t) =>
            IO(println(s"${Console.RED}Error: ${t.getMessage}\n\n${Console.GREEN}${usage}${Console.RESET}"))
          case Copy(_, src, dst) =>
            AwsAPI.copyTable(src, dst)
          case Delete(_, src) =>
            AwsAPI.deleteTable(src)
        }
      }
    } yield r).unsafeRunSync()
  }
}
