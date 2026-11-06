package io.github.adrielc.kyopdf

import io.github.adrielc.bytescan.SharedScan.*
import scala.collection.mutable.ArrayBuffer

/** Bounded ingestion for random-access PDF parsing. The header probe and the
  * decoder share owned input blocks. Full documents retain input until EOF for
  * forward Length references and incremental xrefs, without a flattening copy.
  */
object PdfRead:
  final val MaxInputBytes = 64 * 1024 * 1024
  final val MaxInputBlocks = 65536
  final case class Stats(inputBytes: Long, inputBlocks: Int, peakHeaderBytes: Long)

  private def invalid(message: String): PdfError = PdfError.InvalidPdf(message)
  // Keep only an unfinished marker/version, independent of earlier prefix bytes.
  private val headerLimits = Limits.checked(retainedBytes = 2L * MaxBlockBytes,
    segments = 128, scans = 1, pins = 0).toOption.get
  private final case class HeaderState(search: Int = 0, marker: Int = -1)

  private val header = Probe.stateful[PdfError, HeaderState, String](HeaderState()) { (state, window, eof) =>
    val end = math.min(window.end, 1094L).toInt
    def char(at: Int): Char = (window.read(at.toLong).toOption.get & 255).toChar
    var at = state.search
    var marker = state.marker
    while at <= 1024 && at + 5 <= end && marker < 0 do
      if char(at) == '%' && char(at + 1) == 'P' && char(at + 2) == 'D' && char(at + 3) == 'F' && char(at + 4) == '-' then marker = at
      at += 1
    if marker < 0 && (end >= 1029 || eof) then Step.Failed(invalid("Missing PDF header within the first 1024 bytes"))
    else if marker < 0 then
      val next = math.max(state.search, end - 4)
      Step.Await(HeaderState(next), window.end, next.toLong)
    else
      var cursor = marker + 5
      var dots = 0
      var digits = 0
      var bad = false
      while cursor < end && char(cursor) != '%' && !char(cursor).isWhitespace && !bad do
        val value = char(cursor)
        if value == '.' && digits > 0 && dots == 0 then { dots += 1; digits = 0 }
        else if value >= '0' && value <= '9' then digits += 1
        else bad = true
        cursor += 1
      if bad || cursor - marker - 5 > 64 then Step.Failed(invalid("Invalid PDF version"))
      else if cursor < end || eof then
        if dots != 1 || digits == 0 then Step.Failed(invalid("Invalid PDF version"))
        else Step.Done((marker + 5 until cursor).map(char).mkString)
      else Step.Await(HeaderState(marker, marker), window.end, marker.toLong)
  }

  final class Reader private[PdfRead] (limit: ByteLimit) extends AutoCloseable:
    private val probe = Session.open(header, headerLimits).toOption.get
    private val parts = ArrayBuffer.empty[Block]
    private var size = 0L
    private var failed: Option[PdfError] = None
    private var closed = false
    def stats: Stats = Stats(size, parts.length, probe.stats.peakRetainedBytes)
    private def fail(error: PdfError): Either[PdfError, Nothing] =
      failed = Some(error)
      close()
      Left(error)
    private def headerError(error: Failure[PdfError]): PdfError = error match
      case Failure.Stage(_, value) => value
      case _ => invalid(s"PDF header scan: $error")

    /** Copies each admitted byte once. Bounds are checked before allocation.
      * Arbitrary transport chunk sizes are split into bounded ownership blocks.
      */
    def offer(length: Int)(read: ByteReader): Either[PdfError, Unit] =
      if closed then Left(failed.getOrElse(invalid("PDF input is closed")))
      else if length < 0 then fail(invalid("Negative PDF input chunk length"))
      else if length.toLong > math.min(limit.toLong, MaxInputBytes.toLong) - size then
        fail(PdfError.TooLarge(math.min(limit.toLong, MaxInputBytes.toLong), size + length.toLong))
      else if (length.toLong + MaxBlockBytes - 1) / MaxBlockBytes > MaxInputBlocks - parts.length then
        fail(PdfError.RetentionExceeded(s"PDF input exceeds $MaxInputBlocks blocks"))
      else
        var from = 0
        while from < length && !closed do
          val start = from
          val count = math.min(MaxBlockBytes, length - start)
          val block = Block.copyOf(count)(index => read(start + index)).toOption.get
          parts += block
          size += count
          if probe.result.isEmpty then probe.offer(block) match
            case Left(error) => val _ = fail(headerError(error))
            case Right(_) => ()
          from += count
        failed.toLeft(())

    private def source: Either[PdfError, PdfBytes] =
      if closed then Left(failed.getOrElse(invalid("PDF input is closed")))
      else probe.finish().left.map(headerError).map { _ =>
        val blocks = parts.toVector
        val starts = new Array[Int](blocks.length + 1)
        var i = 0
        while i < blocks.length do
          starts(i + 1) = starts(i) + blocks(i).length
          i += 1
        var cached = 0
        PdfBytes.indexed(size.toInt) { index =>
          if index < starts(cached) || index >= starts(cached + 1) then
            var low = 0
            var high = blocks.length
            while low + 1 < high do
              val middle = (low + high) >>> 1
              if starts(middle) <= index then low = middle else high = middle
            cached = low
          blocks(cached).parserByte(index - starts(cached))
        }
      }

    def scan(): Either[PdfError, ScanReport] =
      val result = source.flatMap(PdfParser.scanBytes(_))
      close()
      result
    def decode(limits: RetentionLimits = RetentionLimits()): Either[PdfError, PdfDocument] =
      val result = source.flatMap(PdfDocumentParser.decodeBytes(_, limits))
      close()
      result
    def close(): Unit =
      closed = true
      probe.close()
      parts.clear()

  def open(limit: ByteLimit): Reader = new Reader(limit)
