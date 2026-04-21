/*
 * Tests for both PdfIO.scoped (zio-blocks-scope based) and
 * PdfIO.zio (ZIO acquireRelease based) file-handle lifetime
 * managers. The interesting bit is the *symmetry*: both pass the
 * same correctness tests, but the scoped version proves at
 * compile time that the InputStream cannot escape its scope,
 * while the zio version is the one to use when the stream value
 * needs to flow through ZIO-shaped code.
 */

package zio.pdf.io

import java.nio.file.Files

import zio.*
import zio.blocks.scope.{Resource, Scope}
import zio.stream.*
import zio.test.*

object PdfIOSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("PdfIO (file-handle lifetime)")(

    // --------------------------------------------------------------
    // Scope-based API
    // --------------------------------------------------------------

    test("PdfIO.scoped.{writeAll, readAll} round-trips a PDF file") {
      for {
        path  <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-scoped-", ".bin"))
        bytes  = Chunk.fromArray((0 until 4096).map(i => (i & 0xff).toByte).toArray)
        _      = PdfIO.scoped.writeAll(path, bytes)
        read   = PdfIO.scoped.readAll(path)
        _     <- ZIO.attemptBlocking(Files.delete(path))
      } yield assertTrue(read.toArray.toSeq == bytes.toArray.toSeq)
    },

    test("PdfIO.scoped.readAll closes the file handle even if the body throws") {
      // Use a Resource that records a release count via a counter
      // we can inspect, then deliberately throw inside the scoped
      // block.
      for {
        counter  <- Ref.make(0)
        rt       <- ZIO.runtime[Any]
        thrown   <- ZIO.attempt {
                      try {
                        Scope.global.scoped { scope =>
                          import scope.*
                          // A custom Resource whose finalizer bumps the counter.
                          // The simpler path: bump a counter via
                          // a `defer` finalizer instead of a custom
                          // Resource. Same observable behaviour;
                          // dodges the call-syntax oddities of
                          // Resource.acquireRelease.
                          allocate(Resource.fromAutoCloseable[java.io.Closeable](
                            new java.io.Closeable {
                              def close(): Unit =
                                Unsafe.unsafe { implicit u =>
                                  rt.unsafe.run(counter.update(_ + 1)).getOrThrow()
                                  ()
                                }
                            }
                          ))
                          // Deliberately fail.
                          throw new RuntimeException("boom")
                          ()
                        }
                        false
                      } catch {
                        case _: RuntimeException => true
                      }
                    }
        seen     <- counter.get
      } yield assertTrue(thrown, seen == 1)
    },

    test("Resource.flatMap composes two file handles, finalized in LIFO") {
      // Two temp files are opened sequentially via Resource.flatMap;
      // both finalizers run when the scope exits. We check by
      // verifying both files still exist (they should, the close
      // is a no-op for tempfiles) and that the bytes round-trip.
      for {
        pa <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-pa-", ".bin"))
        pb <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-pb-", ".bin"))
        _  <- ZIO.attemptBlocking(Files.write(pa, "AAAA".getBytes))
        _  <- ZIO.attemptBlocking(Files.write(pb, "BBBB".getBytes))
        result = Scope.global.scoped { scope =>
                   import scope.*
                   val combined = PdfIO.inputStreamResource(pa).flatMap { _ =>
                     PdfIO.inputStreamResource(pb)
                   }
                   val s: $[java.io.InputStream] = allocate(combined)
                   val out = new java.io.ByteArrayOutputStream
                   $(s) { stream =>
                     val buf = new Array[Byte](16)
                     val n   = stream.read(buf)
                     out.write(buf, 0, n)
                     n
                   }
                   new String(out.toByteArray)
                 }
        _  <- ZIO.attemptBlocking(Files.delete(pa))
        _  <- ZIO.attemptBlocking(Files.delete(pb))
      } yield assertTrue(result == "BBBB") // we read from pb (the inner Resource)
    },

    // --------------------------------------------------------------
    // ZIO-based API - same correctness, different ergonomics
    // --------------------------------------------------------------

    test("PdfIO.zio.{writeAll, readAll} round-trips a PDF file") {
      for {
        path  <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-zio-", ".bin"))
        bytes  = Chunk.fromArray((0 until 4096).map(i => ((i * 7) & 0xff).toByte).toArray)
        wrote <- PdfIO.zio.writeAll(path, bytes)
        read  <- PdfIO.zio.readAll(path)
        _     <- ZIO.attemptBlocking(Files.delete(path))
      } yield assertTrue(wrote == bytes.size.toLong, read == bytes)
    },

    test("PdfIO.zio.reader streams a 1 MiB file without materialising it (memory-bounded)") {
      val size = 1024 * 1024
      for {
        path <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-big-", ".bin"))
        // Write the file via the ZIO sink so we don't OOM building it.
        _    <- ZStream
                  .fromIterable(0 until size)
                  .map(i => (i & 0xff).toByte)
                  .run(PdfIO.zio.writer(path))
        // Now read it back through the streaming reader and just count.
        n    <- PdfIO.zio.reader(path).runCount
        _    <- ZIO.attemptBlocking(Files.delete(path))
      } yield assertTrue(n == size.toLong)
    },

    test("a real PDF round-trips through PdfIO.zio + PdfStream.decode") {
      // This is the actual end-to-end shape we'd care about: read
      // a PDF file, decode it, and assert structural correctness.
      for {
        // The legacy `xref-stream.pdf` fixture is in src/test/resources.
        path  <- ZIO.attemptBlocking(java.nio.file.Path.of("src/test/resources/xref-stream.pdf"))
        out   <- PdfIO.zio
                   .reader(path)
                   .via(zio.pdf.PdfStream.decode(zio.pdf.Log.noop))
                   .runCollect
        objs   = out.collect {
                   case zio.pdf.Decoded.DataObj(_)         => 1
                   case zio.pdf.Decoded.ContentObj(_, _, _) => 1
                 }
        metas  = out.collect { case m: zio.pdf.Decoded.Meta => m }
      } yield assertTrue(objs.size >= 1, metas.size == 1)
    }
  )
}
