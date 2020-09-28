package awsutils
package aws

import java.net.URLEncoder
import java.util.function.BiFunction

import cats.effect.IO
import fs2.Chunk
import cats.effect.concurrent.Semaphore
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import cats.Parallel
import shapeless.tag
import shapeless.tag.@@
import awsutils.io._

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/** AWS utils */
object AwsAPI {
  final abstract class AWSRateLimit
  final abstract class AWSRetries
  final abstract class AWSChunkSize
  final abstract class AWSParallelism

  final case class Config(
      region: Region,
      concurrentCalls: Long @@ AWSRateLimit,
      retries: Int @@ AWSRetries,
      chunkSize: Int @@ AWSChunkSize,
      parallelism: Int @@ AWSParallelism,
      waitOnError: FiniteDuration
  )

  /** All the resources needed to run the following functions.
    * Every program should start by creating a context.
    */
  final case class Context(
      client: S3AsyncClient,
      config: Config,
      semaphore: Semaphore[IO] @@ AWSRateLimit,
      io: IOContext
  ) {
    object impicits {
      implicit val awsContextIOContext: IOContext = io
    }
  }

  object Context {

    /** Create an AWS context to be used by the other functions */
    def create(config: Config)(implicit ioc: IOContext): IO[Context] = {
      import config._
      import ioc.implicits._
      Parallel
        .parMap2(
          IO {
            S3AsyncClient
              .builder()
              .region(region)
              .build()
          },
          Semaphore[IO](concurrentCalls)
        ) { case (c, s) => Context(c, config, tag[AWSRateLimit](s), ioc) }
    }
  }

  /** Implements the retry strategy: number of retries, waiting duration between them.
    * Settings configured via the context.
    */
  def retry[A](io: IO[A])(implicit context: Context): IO[A] =
    IOContext.retry(context.config.retries, context.config.waitOnError)(io)(context.io)

  ////////////////////////
  //  Aws API Bindings  //
  ////////////////////////

  /** List objects of this bucket under this prefix */
  def listObjects(s3TableURL: S3TableURL, startingToken: Option[String])(implicit
      context: Context
  ): IO[ListObjectsV2Response] =
    context.semaphore.withPermit {
      val request: ListObjectsV2Request =
        ListObjectsV2Request
          .builder()
          .bucket(s3TableURL.bucket)
          .maxKeys(1000)
          .when(s3TableURL.prefixOpt, (r, p: String) => r.prefix(p))
          .when(startingToken, (r, t: String) => r.continuationToken(t))
          .build()

      retry(IO.async { k =>
        context.client
          .listObjectsV2(request)
          .handleAsync[Unit](new BiFunction[ListObjectsV2Response, Throwable, Unit] {
            def apply(list: ListObjectsV2Response, exn: Throwable): Unit =
              if (list.nonNull)
                k(Right(list))
              else if (exn.nonNull)
                k(Left(exn))
              else
                k(Left(new Exception("Returned null list and exception")))
          })
        ()
      })
    }

  def copyObject(sourceBucket: String, s3Object: S3Object, destinationBucket: String, destinationKey: String)(implicit
      aws: Context
  ): IO[CopyObjectResponse] = {
    aws.semaphore.withPermit {
      val request: CopyObjectRequest =
        CopyObjectRequest
          .builder()
          .copySource(URLEncoder.encode(s"${sourceBucket}/${s3Object.key()}", "UTF-8"))
          .destinationBucket(destinationBucket)
          .destinationKey(destinationKey)
          .storageClass(s3Object.storageClassAsString())
          .build()

      retry(IO.async[CopyObjectResponse] { k =>
        aws.client
          .copyObject(request)
          .handleAsync[Unit](new BiFunction[CopyObjectResponse, Throwable, Unit] {
            def apply(t: CopyObjectResponse, u: Throwable): Unit =
              if (u.nonNull)
                k(Left(u))
              else if (t.nonNull)
                k(Right(t))
              else
                k(Left(new Exception("Returned null response and exception")))
          })
        ()
      })
    }
  }

  def deleteObjects(bucket: String, objects: Iterator[S3Object])(implicit
      aws: Context
  ): IO[Option[DeleteObjectsResponse]] =
    if (objects.isEmpty)
      IO.pure(None)
    else
      aws.semaphore.withPermit {
        val objectsCollection: java.util.Collection[ObjectIdentifier] =
          objects
            .map { (obj: S3Object) =>
              ObjectIdentifier
                .builder()
                .key(obj.key())
                .build()
            }
            .toList
            .asJava
        val delete: Delete =
          Delete
            .builder()
            .quiet(true)
            .objects(objectsCollection)
            .build()
        val deleteObjectsRequest =
          DeleteObjectsRequest
            .builder()
            .bucket(bucket)
            .delete(delete)
            .build()

        retry(
          IO.async { (k: Either[Throwable, DeleteObjectsResponse] => Unit) =>
              aws.client
                .deleteObjects(deleteObjectsRequest)
                .handleAsync[Unit](new BiFunction[DeleteObjectsResponse, Throwable, Unit] {
                  def apply(t: DeleteObjectsResponse, u: Throwable): Unit =
                    if (u.nonNull) {
                      u.addSuppressed(new Exception(s"Failed request ${deleteObjectsRequest.toString}."))
                      k(Left(u))
                    } else if (t.nonNull)
                      k(Right(t))
                    else
                      k(Left(new Exception("Returned null response and exception")))
                })
              ()
            }
            .flatMap { (response: DeleteObjectsResponse) =>
              if (response.hasErrors) {
                val errors =
                  response
                    .errors()
                    .iterator()
                    .asScala
                    .map(x => s"${x.code()}: ${x.message()} on ${x.key()} (version ${x.versionId()}) ${x.toString}")
                    .mkString("\n")
                IO.raiseError(new Exception(s"Errors when deleting objects on bucket ${bucket}:\n${errors}"))
              } else IO.pure(Some(response))
            }
        )
      }

  ////////////////////
  //  Streaming API //
  ////////////////////

  /** The stream of all objects in this bucket, under this prefix */
  def objectStream(s3TableURL: S3TableURL)(implicit context: Context): fs2.Stream[IO, S3Object] = {
    sealed abstract class State extends Product with Serializable
    object State {
      case object Start                                 extends State
      final case class Next(calls: Long, token: String) extends State
      case object End                                   extends State
    }
    fs2.Stream.unfoldChunkEval[IO, State, S3Object](State.Start) {
      case State.End =>
        IO.pure(None)
      case st =>
        val (calls, token) = st match {
          case State.Next(c, t) => (c, Some(t))
          case _                => (0L, None)
        }
        for {
          _    <- IO(println(s"    [${s3TableURL.bucket}] ${s3TableURL.prefix} -> ${(calls + 1L).toString}"))
          resp <- listObjects(s3TableURL, token)
        } yield {
          val newState = Option(resp.nextContinuationToken()) match {
            case Some(t) => State.Next(calls + 1L, t)
            case _       => State.End
          }
          Some((Chunk.seq(resp.contents().asScala), newState))
        }
    }
  }

  def isEmpty(s3TableURL: S3TableURL)(implicit aws: Context): IO[Boolean] =
    listObjects(s3TableURL, None).map(!_.hasContents)

  def deleteStream(s3TableURL: S3TableURL)(implicit aws: Context): fs2.Stream[IO, DeleteObjectsResponse] = {
    import aws.config._
    import aws.io.implicits._
    objectStream(s3TableURL)
      .chunkLimit(chunkSize)
      .parEvalMapUnordered(parallelism) { (chunk: Chunk[S3Object]) =>
        deleteObjects(s3TableURL.bucket, chunk.iterator)
      }
      .collect {
        case Some(x) => x
      }
  }

  def deleteTable(s3TableURL: S3TableURL)(implicit aws: Context): IO[Unit] = deleteStream(s3TableURL).compile.drain

  /** Copy all objects from a table to another table.
    * Destination table is NOT WIPED before copying so any object present before the copy
    * in the destination table will stay there.
    *
   * The copy support rewriting the relative path (the substring of the key without the source table prefix).
    * This is useful to alter the partition scheme. The copy fails if the [[pathRewriter]] returns <code>None</code>.
    * This is useful to detect errors when source object are not what you expect.
    *
   * @param src the source table the objects will be copied from.
    * @param dst the destination table the objects will be copied to.
    * @param pathRewriter the function that takes the relative path of the source object
    *                     (the substring of the key without the source table prefix).
    *                     If it returns <code>None</code> for some input, then the copy fails!
    * @param aws
    * @return
    */
  def copyStream(src: S3TableURL, dst: S3TableURL, pathRewriter: String => Option[String] = Some(_))(implicit
      aws: Context
  ): fs2.Stream[IO, CopyObjectResponse] = {
    import aws.config._
    import aws.io.implicits._
    val srcPrefixLength = src.prefix.size
    val dstPrefix       = dst.prefix
    objectStream(src)
      .parEvalMapUnordered(parallelism) { s3Object =>
        val relativePath = s3Object.key().drop(srcPrefixLength)
        pathRewriter(relativePath) match {
          case Some(path) =>
            val dstKey = dstPrefix + path
            copyObject(src.bucket, s3Object, dst.bucket, dstKey)
          case _ =>
            IO.raiseError(new Exception(s"Error in path rewriting for ${relativePath}"))
        }
      }
  }

  /** See [[copyStream]] */
  def copyTable(src: S3TableURL, dst: S3TableURL, pathRewriter: String => Option[String] = Some(_))(implicit
      aws: Context
  ): IO[Unit] =
    copyStream(src, dst, pathRewriter).compile.drain
}
