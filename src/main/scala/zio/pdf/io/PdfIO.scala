/*
 * File-handle lifetime, two ways:
 *
 *   - `PdfIO.scoped.{readAll, writeAll}` uses zio-blocks-scope's
 *     `Resource` + `Scope` + the `$` macro. Compile-time leak
 *     prevention, zero ZIO dependency, but the synchronous,
 *     scope-bounded API forces the work to happen inline (the file
 *     handle cannot escape the `scoped` block, so `ZStream` /
 *     `ZSink` must be built and consumed inside it). Best for
 *     small / medium PDFs where you want the strongest possible
 *     "did I forget to close this?" guarantee.
 *
 *   - `PdfIO.zio.{readAll, writeAll, reader, writer}` uses ZIO's
 *     own `ZIO.acquireRelease` / `ZStream.fromInputStream`. The
 *     stream value can flow freely through `ZIO`-shaped code; the
 *     file handle's lifetime is tied to a `ZIO.scoped` block at the
 *     call site. Best for large files, async workflows, anything
 *     that wants `ZStream` composition past the function boundary.
 *
 * Both APIs guarantee LIFO finalization on success / failure /
 * defect. Pick the one that matches your call site.
 *
 * What this DOES NOT do: erase ZIO. The streaming substrate
 * (ZStream / ZPipeline / ZChannel) stays for the actual
 * decode/encode work. `Resource` only owns the file handle.
 */

package zio.pdf.io

import java.io.{InputStream, OutputStream}
import java.nio.file.{Files, Path, StandardOpenOption}

import zio.{Chunk, ZIO}
import zio.blocks.scope.{Resource, Scope, Unscoped}
import zio.stream.{ZSink, ZStream}

object PdfIO {

  // =================================================================
  // Resource constructors (used by both the scoped and zio APIs)
  // =================================================================

  /** A file's `InputStream` as a `Resource`, finalized when the
    * enclosing `zio.blocks.scope.Scope` exits. */
  def inputStreamResource(path: Path): Resource[InputStream] =
    Resource.fromAutoCloseable[InputStream](Files.newInputStream(path))

  /** A file's `OutputStream` as a `Resource`. Default options are
    * CREATE + TRUNCATE_EXISTING + WRITE. */
  def outputStreamResource(
    path: Path,
    options: StandardOpenOption*
  ): Resource[OutputStream] = {
    val opts: Array[StandardOpenOption] =
      if (options.isEmpty)
        Array(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
      else
        options.toArray
    Resource.fromAutoCloseable[OutputStream](Files.newOutputStream(path, opts*))
  }

  // =================================================================
  // The Scope-based API: compile-time leak prevention,
  // synchronous, scope-bounded.
  // =================================================================

  object scoped {

    // `Chunk[Byte]` is pure data (immutable byte buffer); safe to
    // return from a Scope. zio-blocks's Unscoped derivation can't
    // see inside zio.Chunk's class hierarchy, so we provide the
    // instance explicitly.
    private given Unscoped[Chunk[Byte]] = new Unscoped[Chunk[Byte]] {}
    private given Unscoped[Long]         = new Unscoped[Long] {}

    /**
     * Read `path` synchronously into a single `Chunk[Byte]`. The
     * file handle is owned by a `zio.blocks.scope.Scope`, so it
     * is closed in LIFO order when this method returns - the macro
     * in the `$` accessor *guarantees at compile time* that the
     * InputStream cannot escape the scoped block.
     */
    def readAll(path: Path, chunkSize: Int = 64 * 1024): Chunk[Byte] =
      Scope.global.scoped { scope =>
        import scope.*
        val is: $[InputStream] = allocate(inputStreamResource(path))
        // The whole read loop happens inside the `$` macro - the
        // InputStream is only ever used as a receiver, never
        // captured or returned.
        val out = new java.io.ByteArrayOutputStream(math.min(chunkSize * 4, 4 * 1024 * 1024))
        $(is) { stream =>
          val buf = new Array[Byte](chunkSize)
          var n   = stream.read(buf)
          while (n >= 0) {
            out.write(buf, 0, n)
            n = stream.read(buf)
          }
          out.size()
        }
        Chunk.fromArray(out.toByteArray)
      }

    /**
     * Write `bytes` synchronously to `path`. The OutputStream's
     * lifetime is bounded by a `Scope`. Returns the number of
     * bytes written.
     */
    def writeAll(path: Path, bytes: Chunk[Byte], options: StandardOpenOption*): Long =
      Scope.global.scoped { scope =>
        import scope.*
        val os: $[OutputStream] = allocate(outputStreamResource(path, options*))
        val arr                  = bytes.toArray
        $(os)(stream => stream.write(arr))
        arr.length.toLong
      }
  }

  // =================================================================
  // The ZIO-based API: streaming file I/O, ZIO-native lifetime.
  // =================================================================

  object zio {

    /** A `ZStream[Byte]` reading from `path`. The InputStream is
      * acquired/released by ZStream's own scope when run. */
    def reader(path: Path, chunkSize: Int = 64 * 1024): ZStream[Any, Throwable, Byte] =
      ZStream.fromInputStreamZIO(
        _root_.zio.ZIO
          .attemptBlocking(Files.newInputStream(path))
          .refineToOrDie[java.io.IOException],
        chunkSize
      )

    /** A `ZSink[Byte, Long]` writing to `path`. The OutputStream
      * is acquired/released by `ZSink.fromOutputStreamScoped`. */
    def writer(path: Path, options: StandardOpenOption*): ZSink[Any, Throwable, Byte, Byte, Long] = {
      val opts: Array[StandardOpenOption] =
        if (options.isEmpty)
          Array(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        else
          options.toArray
      ZSink.fromOutputStreamScoped(
        _root_.zio.ZIO.fromAutoCloseable(
          _root_.zio.ZIO
            .attemptBlocking(Files.newOutputStream(path, opts*))
            .refineToOrDie[java.io.IOException]
        )
      )
    }

    /** Convenience: read whole file as a Chunk (lifetime auto-managed). */
    def readAll(path: Path, chunkSize: Int = 64 * 1024): _root_.zio.ZIO[Any, Throwable, Chunk[Byte]] =
      reader(path, chunkSize).runCollect

    /** Convenience: write whole Chunk to file (lifetime auto-managed). */
    def writeAll(path: Path, bytes: Chunk[Byte], options: StandardOpenOption*): _root_.zio.ZIO[Any, Throwable, Long] =
      ZStream.fromChunk(bytes).run(writer(path, options*))
  }
}
