package awsutils
package io

import java.io.{File, PrintWriter}
import cats.effect._

object FS {

  /** Use {{{withFile(fileName}.use { printFile => ... }}}
    *  to open the file named `fileName` and write to it.
    *  Use the function `printFile` (the value of the resource
    *  to actually print into this file.
    *  The file is automatically closed on completion or error.
    *
    *  @param fileName the name of the file to open
    *  @return the function `printFile` to add a line to the file.
    *          NOTE: new lines are automatically added just like [[println]].
    */
  def withFile(fileName: String): Resource[IO, String => IO[Unit]] =
    Resource {
      IO(new PrintWriter(new File(fileName)))
        .map { (pw: PrintWriter) => ((s: String) => IO({ pw.println(s); pw.flush() }), IO(pw.close())) }
    }
}
