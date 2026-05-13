/*
 * ZIO port of fs2.pdf.WriteFile — drain a byte stream to a path.
 */

package zio.pdf.testkit

import java.nio.file.Path

import zio.*
import zio.stream.*

object WriteFile {

  def run(path: Path)(bytes: ZStream[Any, Throwable, Byte]): Task[Unit] =
    bytes.run(ZSink.fromFileName(path.toString)).unit
}
