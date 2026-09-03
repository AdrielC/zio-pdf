/*
 * Port of fs2.pdf.JavaStream — byte-array stream helpers for JVM interop.
 */

package zio.pdf

import java.io.*

import zio.*

private[pdf] object JavaStream {

  def baos: ByteArrayOutputStream =
    new ByteArrayOutputStream()

  def withBaos(f: OutputStream => Task[Unit]): Task[Array[Byte]] =
    ZIO.succeed(baos).flatMap { os =>
      f(os) *> ZIO.succeed(os.toByteArray)
    }

  def bais(bytes: Array[Byte]): ByteArrayInputStream =
    new ByteArrayInputStream(bytes)

  def withBaisStrict[A](bytes: Array[Byte])(f: InputStream => Task[A]): Task[A] =
    ZIO.acquireReleaseWith(ZIO.succeed(bais(bytes)))(is => ZIO.succeed(is.close()))(f)

  def withByteStreams(bytes: Array[Byte])(f: (InputStream, OutputStream) => Task[Unit]): Task[Array[Byte]] =
    withBaisStrict(bytes)(is => withBaos(os => f(is, os)))
}
