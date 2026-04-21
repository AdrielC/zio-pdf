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
 *
 * `PdfIO.scoped.decode*` / `validate` / `comparePaths` drive
 * [[zio.pdf.PdfStream]] synchronously: the [[InputStream]] is only a
 * `read` receiver inside the `$` block, and bytes are fed chunk-by-chunk
 * through the same decoders as `ZStream.via(PdfStream.*)` (no full-file
 * `ByteArrayOutputStream`). The returned `Chunk[Decoded]` / validation
 * still holds all decoded values in memory like `runCollect` would.
 */

package zio.pdf.io

import java.io.{InputStream, OutputStream}
import java.nio.file.{Files, Path, StandardOpenOption}

import zio.{Chunk, Runtime, Unsafe, ZIO}
import zio.blocks.scope.{Resource, Scope, Unscoped}
import zio.pdf.*
import zio.prelude.Validation
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
    // instance explicitly. Each needs a distinct given name — Scala
    // encodes `given Unscoped[Chunk[A]]` as the same synthetic name
    // for different `A`.
    private given chunkByteUnscoped: Unscoped[Chunk[Byte]] =
      new Unscoped[Chunk[Byte]] {}
    private given longUnscoped: Unscoped[Long] =
      new Unscoped[Long] {}
    private given chunkStreamingDecodedUnscoped: Unscoped[Chunk[StreamingDecoded]] =
      new Unscoped[Chunk[StreamingDecoded]] {}
    private given chunkDecodedUnscoped: Unscoped[Chunk[Decoded]] =
      new Unscoped[Chunk[Decoded]] {}
    private given validationPdfUnscoped: Unscoped[Validation[PdfError, Unit]] =
      new Unscoped[Validation[PdfError, Unit]] {}
    private def unsafeRun[A](zio: ZIO[Any, Throwable, A]): A =
      Unsafe.unsafe { implicit u =>
        Runtime.default.unsafe.run(zio).getOrThrowFiberFailure()
      }

    private def chunkFromReadBuffer(buf: Array[Byte], n: Int): Chunk[Byte] = {
      val copy = java.util.Arrays.copyOf(buf, n)
      Chunk.fromArray(copy)
    }

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

    /**
     * Decode `path` through [[PdfStream.streamingDecode]]. The
     * [[InputStream]] is only used as the receiver of `read` inside
     * the `$` block (zio-blocks-scope rules); bytes are fed through the
     * streaming decoder in `chunkSize` pieces without materialising the
     * whole file first.
     */
    def decodeStreamingDecoded(
      path: Path,
      chunkSize: Int = 64 * 1024,
      log: Log = Log.noop,
      config: StreamingDecode.Config = StreamingDecode.Config.default
    ): Chunk[StreamingDecoded] =
      Scope.global.scoped { scope =>
        import scope.*
        val is: $[InputStream] = allocate(inputStreamResource(path))
        $(is) { stream =>
          val buf  = new Array[Byte](chunkSize)
          var fs   = StreamingDecode.initialFinalState
          val outB = Chunk.newBuilder[StreamingDecoded]
          var n    = stream.read(buf)
          while (n > 0) {
            val (evs, fs1) = StreamingDecode.stepChunk(config, fs, chunkFromReadBuffer(buf, n))
            fs = fs1
            if (evs.nonEmpty) outB ++= evs
            n = stream.read(buf)
          }
          if (n < 0) {
            val meta = unsafeRun(StreamingDecode.finalizeToMeta(log, fs))
            outB ++= meta
          }
          outB.result()
        }
      }

    /**
     * Decode `path` through [[PdfStream.decode]] (full `Decoded` layer).
     * Same incremental read shape as [[decodeStreamingDecoded]].
     */
    def decodeDecoded(
      path: Path,
      chunkSize: Int = 64 * 1024,
      log: Log = Log.noop,
      config: StreamingDecode.Config = StreamingDecode.Config.default
    ): Chunk[Decoded] =
      Scope.global.scoped { scope =>
        import scope.*
        val is: $[InputStream] = allocate(inputStreamResource(path))
        $(is) { stream =>
          val buf  = new Array[Byte](chunkSize)
          var sDec = DecodedFromStreaming.accInitial
          var fs   = StreamingDecode.initialFinalState
          val outB = Chunk.newBuilder[Decoded]
          var n    = stream.read(buf)
          while (n > 0) {
            PdfStream.decodeSyncStep(config)(sDec, fs, chunkFromReadBuffer(buf, n)) match {
              case Left(err) => throw err
              case Right((d, a, f)) =>
                sDec = a
                fs = f
                if (d.nonEmpty) outB ++= d
            }
            n = stream.read(buf)
          }
          if (n < 0) {
            val tail = unsafeRun(PdfStream.decodeSyncFinish(log)(sDec, fs))
            if (tail.nonEmpty) outB ++= tail
          }
          outB.result()
        }
      }

    /** [[PdfStream.validate]] for a file path (scope-owned input). */
    def validate(
      path: Path,
      chunkSize: Int = 64 * 1024,
      log: Log = Log.noop
    ): Validation[PdfError, Unit] =
      Scope.global.scoped { scope =>
        import scope.*
        val is: $[InputStream] = allocate(inputStreamResource(path))
        $(is) { stream =>
          val buf  = new Array[Byte](chunkSize)
          var sDec = DecodedFromStreaming.accInitial
          var fs   = StreamingDecode.initialFinalState
          val outB = Chunk.newBuilder[Decoded]
          var n    = stream.read(buf)
          while (n > 0) {
            PdfStream.decodeSyncStep(StreamingDecode.Config.default)(sDec, fs, chunkFromReadBuffer(buf, n)) match {
              case Left(err) => throw err
              case Right((d, a, f)) =>
                sDec = a
                fs = f
                if (d.nonEmpty) outB ++= d
            }
            n = stream.read(buf)
          }
          if (n < 0) {
            val tail = unsafeRun(PdfStream.decodeSyncFinish(log)(sDec, fs))
            if (tail.nonEmpty) outB ++= tail
          }
          val decoded = outB.result()
          unsafeRun(ValidatePdf.fromDecoded(ZStream.fromChunk(decoded)))
        }
      }

    /**
     * [[PdfStream.compare]] for two paths. Each file is decoded with an
     * incremental scoped read; comparison uses the collected `Decoded`
     * chunks (same as `runCollect` on `via(PdfStream.decode)`).
     */
    def comparePaths(
      oldPath: Path,
      newPath: Path,
      chunkSize: Int = 64 * 1024,
      log: Log = Log.noop
    ): Validation[CompareError, Unit] = {
      val oldDecoded = decodeDecoded(oldPath, chunkSize, log)
      val newDecoded = decodeDecoded(newPath, chunkSize, log)
      unsafeRun(
        ComparePdfs.fromDecoded(ZStream.fromChunk(oldDecoded), ZStream.fromChunk(newDecoded))
      )
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
