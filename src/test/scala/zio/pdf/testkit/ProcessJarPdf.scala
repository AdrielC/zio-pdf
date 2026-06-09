/*
 * ZIO port of fs2.pdf.ProcessJarPdf — run a pipeline against a bundled PDF.
 */

package zio.pdf.testkit

import zio.*
import zio.stream.*

object ProcessJarPdf {

  def processWith[A](doc: String)(
      f: Boolean => ZPipeline[Any, Throwable, Byte, A]
  ): ZIO[Any, JarError, ZStream[Any, Throwable, A]] =
    Jar.resourceStream(s"$doc.pdf").map { case (bytes, _) => bytes.via(f(false)) }

  def processWithIO[R, E, A](doc: String)(
      f: Boolean => ZStream[Any, Throwable, Byte] => ZIO[R, E, A]
  ): ZIO[R, E | JarError, A] =
    Jar.resourceStream(s"$doc.pdf").mapError(identity).flatMap { case (bytes, _) => f(false)(bytes) }

  def ignoreDrain[A](stream: ZIO[Any, JarError, ZStream[Any, Throwable, A]]): Task[Unit] =
    stream.foldCauseZIO(
      _ => ZIO.unit,
      _.runDrain
    )
}
