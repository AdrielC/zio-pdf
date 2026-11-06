package io.github.adrielc.kyopdf.zio

import kyo.{Abort, Async, ZIOs, <}
import io.github.adrielc.kyopdf.{ByteLimit, ContentToken, ObjectRef, PdfContentParser, PdfDocument, PdfError, PdfRead, PdfObject, PdfPages, PdfParser, PdfWriter, RetentionLimits, ScanReport}
import _root_.zio.{IO, ZIO}
import _root_.zio.stream.ZStream

/** ZIO compatibility edge for the Kyo-native PDF core.
  *
  * Typed Kyo `Abort` failures remain typed ZIO failures. Kyo panics remain
  * defects, and interruption is delegated to the Kyo fiber by `ZIOs.run`.
  */
object PdfZIO:
  def run[E, A](effect: => A < (Abort[E] & Async)): IO[E, A] =
    ZIOs.run(effect)

  def scan(bytes: Array[Byte], limit: ByteLimit): IO[PdfError, ScanReport] =
    run(Async.defer(PdfParser.scan(bytes, limit)))

  def validate(report: ScanReport, limits: RetentionLimits): IO[PdfError, Unit] =
    run(Async.defer(PdfParser.validate(report, limits)))

  def decode(bytes: Array[Byte], limit: ByteLimit, limits: RetentionLimits = RetentionLimits()): IO[PdfError, PdfDocument] =
    run(Async.defer(PdfDocument.decode(bytes, limit, limits)))

  /** Shared owned blocks serve header inspection and document decoding. Header
    * and admission failures stop upstream pulls, and scope exit releases input.
    */
  def decodeStream[R, E](
    source: ZStream[R, E, Byte],
    limit: ByteLimit,
    limits: RetentionLimits = RetentionLimits()
  ): ZIO[R, E | PdfError, PdfDocument] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.succeed(PdfRead.open(limit)))(reader => ZIO.succeed(reader.close())).flatMap { reader =>
        source.chunks.runForeach(bytes => ZIO.fromEither(reader.offer(bytes.length)(bytes.apply)))
          .flatMap(_ => ZIO.fromEither(reader.decode(limits)))
      }
    }

  def pageRefs(document: PdfDocument): IO[PdfError, Vector[ObjectRef]] =
    run(Async.defer(PdfPages.pageRefs(document)))

  def decodedStream(obj: PdfObject, maxBytes: Int = 512 * 1024): IO[PdfError, Vector[Byte]] =
    run(Async.defer(PdfDocument.decodedStream(obj, maxBytes)))

  def select(document: PdfDocument, first: Int, last: Int): IO[PdfError, PdfDocument] =
    run(Async.defer(PdfPages.select(document, first, last)))

  def write(document: PdfDocument): IO[PdfError, Vector[Byte]] =
    run(Async.defer(PdfWriter.write(document)))

  def selectPagesStream[R, E](
    source: ZStream[R, E, Byte],
    limit: ByteLimit,
    first: Int,
    last: Int,
    limits: RetentionLimits = RetentionLimits()
  ): ZStream[R, E | PdfError, Byte] =
    ZStream.unwrap(
      decodeStream(source, limit, limits)
        .flatMap(select(_, first, last))
        .flatMap(write)
        .map(ZStream.fromIterable)
    )

  def parseContent(bytes: Array[Byte], limits: PdfContentParser.Limits = PdfContentParser.Limits()): IO[PdfError, Vector[ContentToken]] =
    run(Async.defer(PdfContentParser.parse(bytes, limits)))

  /** Incremental bounded input with an early header probe. It retains admitted
    * chunks for xrefs, then scans them without runCollect or a flattening copy.
    */
  def scanStream[R, E](source: ZStream[R, E, Byte], limit: ByteLimit): ZIO[R, E | PdfError, ScanReport] =
    ZIO.scoped {
      ZIO.acquireRelease(ZIO.succeed(PdfRead.open(limit)))(reader => ZIO.succeed(reader.close())).flatMap { reader =>
        source.chunks.runForeach(bytes => ZIO.fromEither(reader.offer(bytes.length)(bytes.apply)))
          .flatMap(_ => ZIO.fromEither(reader.scan()))
      }
    }
