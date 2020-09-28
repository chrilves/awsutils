package awsutils
package io

import java.io.{File, FileWriter, PrintWriter, StringWriter, Writer}

import cats.effect._
import cats.effect.concurrent.Semaphore

object FS {

  /** Use {{{withFile(fileName}.use { printFile => ... }}}
    *  to open the file named `fileName` and write to it.
    *  Use the function `printFile` (the value of the resource
    *  to actually print into this file.
    *  The file is automatically closed on completion or error.
    *
   *  Unlike [[withFileTransactional]], the file is created eagerly
    *  and filled on the fly.
    *
   *  @param filePath the name of the file to open
    *  @return the function `printFile` to add a line to the file.
    *          NOTE: new lines are automatically added just like [[println]].
    */
  def withFile(filePath: String)(implicit ioc: IOContext): Resource[IO, String => IO[Unit]] =
    Resource {
      import ioc.implicits._
      for {
        mutex <- Semaphore[IO](1)
        pw    <- IO(new PrintWriter(new File(filePath)))
      } yield ((s: String) => mutex.withPermit(IO({ pw.println(s); pw.flush() })), IO(pw.close()))
    }

  /** Use {{{withFileTransactional(fileName}.use { printFile => ... }}}
    *  to open the file named `fileName` and write to it.
    *  Use the function `printFile` (the value of the resource
    *  to actually print into this file.
    *  The file is automatically closed on completion or error.
    *
   *  Unlike [[withFile]], the file is created and filled only when
    *  all the lines have been collected.
    *
   *  @param filePath the name of the file to open
    *  @return the function `printFile` to add a line to the file.
    *          NOTE: new lines are automatically added just like [[println]].
    */
  def withFileTransactional(filePath: String)(implicit ioc: IOContext): Resource[IO, String => IO[Unit]] = {
    import ioc.implicits._
    for {
      sw <- Resource[IO, StringWriter](IO(new StringWriter()).map(sw => sw -> IO.pure(())))
      mu <- Resource[IO, Semaphore[IO]](Semaphore[IO](1).map(s => s -> IO.pure(())))
      pw <- printWriter(sw)
      _ <- Resource[IO, Unit] {
        IO(
          () -> fileWriter(filePath).use(fw =>
            IO {
              pw.flush()
              fw.write(sw.toString())
              fw.flush()
            }
          )
        )
      }
    } yield (s: String) => mu.withPermit(IO(pw.println(s)))
  }

  def fileWriter(filePath: String): Resource[IO, FileWriter] =
    Resource[IO, FileWriter](IO(new FileWriter(filePath)).map(fw => fw -> IO(fw.close())))

  def printWriter(w: Writer): Resource[IO, PrintWriter] =
    Resource[IO, PrintWriter](IO(new PrintWriter(w)).map(pw => pw -> IO(pw.close())))
}
