package zio.pdf.io

import java.nio.file.Files

import zio.*
import zio.pdf.PdfStream
import zio.stream.*
import zio.test.*

object PdfIOSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("PdfIO (ZIO file I/O)")(
    test("writeAll + readAll round-trips a file") {
      for {
        path  <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-", ".bin"))
        bytes  = Chunk.fromArray((0 until 4096).map(i => ((i * 7) & 0xff).toByte).toArray)
        wrote <- PdfIO.writeAll(path, bytes)
        read  <- PdfIO.readAll(path)
        _     <- ZIO.attemptBlocking(Files.delete(path))
      } yield assertTrue(wrote == bytes.size.toLong, read == bytes)
    },
    test("reader streams a 1 MiB file without materialising it") {
      val size = 1024 * 1024
      for {
        path <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-big-", ".bin"))
        _    <- ZStream
                  .fromIterable(0 until size)
                  .map(i => (i & 0xff).toByte)
                  .run(PdfIO.writer(path))
        n    <- PdfIO.reader(path).runCount
        _    <- ZIO.attemptBlocking(Files.delete(path))
      } yield assertTrue(n == size.toLong)
    },
    test("decodeDecoded on xref-stream.pdf") {
      val path = java.nio.file.Path.of("src/test/resources/xref-stream.pdf")
      for {
        out <- PdfIO.decodeDecoded(path)
        objs = out.collect {
          case zio.pdf.Decoded.DataObj(_)          => 1
          case zio.pdf.Decoded.ContentObj(_, _, _) => 1
        }
        metas = out.collect { case m: zio.pdf.Decoded.Meta => m }
      } yield assertTrue(objs.size >= 1, metas.size == 1)
    }
  )
}
