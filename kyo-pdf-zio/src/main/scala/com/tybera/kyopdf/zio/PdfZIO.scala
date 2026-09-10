package com.tybera.kyopdf.zio

import kyo.{Abort, Async, ZIOs, <}
import com.tybera.kyopdf.{ByteLimit, ContentToken, ObjectRef, PdfContentParser, PdfDocument, PdfError, PdfInput, PdfObject, PdfPages, PdfParser, PdfWriter, RetentionLimits, ScanReport}
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

  /** Incrementally consumes ZStream chunks into the bounded Kyo input state.
    * The adapter never invokes `runCollect`; only the final random-access PDF
    * representation is materialized by `PdfInput.finish`.
    */
  def decodeStream[R, E](
    source: ZStream[R, E, Byte],
    limit: ByteLimit,
    limits: RetentionLimits = RetentionLimits()
  ): ZIO[R, E | PdfError, PdfDocument] =
    source.chunks.runFoldZIO(PdfInput.empty(limit)) { (input, bytes) =>
      run(Async.defer(input.feed(bytes.toArray)))
    }.flatMap(input => run(Async.defer(input.finish(limits))))

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

  /** Bounded compatibility adapter for an existing ZStream application.
    *
    * The Kyo core owns an `Array[Byte]`, so this adapter collects no more than
    * the configured limit plus one byte. A future Kyo Stream parser can replace
    * this adapter without changing the ZIO-facing result type.
    */
  def scanStream[R, E](source: ZStream[R, E, Byte], limit: ByteLimit): ZIO[R, E | PdfError, ScanReport] =
    val maximum = limit.toLong
    val take = if maximum == Long.MaxValue then Long.MaxValue else maximum + 1L
    source.take(take).runCollect.flatMap { bytes =>
      if bytes.length.toLong > maximum then ZIO.fail(PdfError.TooLarge(maximum, bytes.length.toLong))
      else scan(bytes.toArray, limit)
    }
