package com.tybera.kyopdf.zio

import kyo.{Abort, Async, ZIOs, <}
import com.tybera.kyopdf.{ByteLimit, ContentToken, PdfContentParser, PdfError, PdfParser, RetentionLimits, ScanReport}
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
