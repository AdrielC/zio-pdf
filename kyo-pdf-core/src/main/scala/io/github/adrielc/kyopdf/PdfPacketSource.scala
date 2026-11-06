package io.github.adrielc.kyopdf

import java.io.{IOException, RandomAccessFile}
import java.nio.file.Path
import kyo.*

/** A scoped, seekable packet input. Only syntax is decoded; ordinary stream
  * payloads remain file ranges. The caller owns the file and must keep it
  * unchanged until the append pull reaches EOF, then close this handle.
  * Instances are single-consumer, as are packet sessions.
  */
final class PdfPacketSource private (file: RandomAccessFile, val size: Long, bounds: PdfPacket.Bounds)
    extends AutoCloseable:
  private val window = new Array[Byte](bounds.windowBytes)
  private var start = -1L
  private var count = 0
  private var loads = 0L
  private var closed = false
  private var indexed: Option[(PdfDocument, Map[ObjectRef, PdfDocumentParser.Span])] = None
  private def byteAt(position: Long): Byte =
    if closed then throw new IOException("Packet source is closed")
    if position < 0 || position >= size then throw new IOException("Packet source offset is outside input")
    if position < start || position - start >= count then
      start = position - position % window.length
      count = math.min(window.length.toLong, size - start).toInt
      file.seek(start)
      file.readFully(window, 0, count)
      loads += 1
    window((position - start).toInt)
  private def index(): Either[PdfError, Unit] =
    val input = PdfBytes.indexed(size, math.min(Int.MaxValue.toLong, bounds.metadataBytes).toInt)(byteAt)
    PdfDocumentParser.decodeIndexed(input, bounds.retention).map { value => indexed = Some(value) }
  private def active: (PdfDocument, Map[ObjectRef, PdfDocumentParser.Span]) =
    indexed.getOrElse(throw new IOException("Packet source is closed or not indexed"))
  private[kyopdf] def document: PdfDocument = active._1
  private[kyopdf] def streams: Map[ObjectRef, PdfDocumentParser.Span] = active._2
  private[kyopdf] def bytes(range: PdfDocumentParser.Span): Iterator[Byte] = new Iterator[Byte]:
    private var position = range.start
    private val end = range.start + range.length
    def hasNext: Boolean = position < end
    def next(): Byte =
      if !hasNext then throw new NoSuchElementException("Packet payload exhausted")
      val value = byteAt(position)
      position += 1
      value
  def stats: PdfPacketSource.Stats = PdfPacketSource.Stats(window.length, loads,
    indexed.fold(0L)(_._2.valuesIterator.map(_.length).sum), indexed.fold(0)(_._1.objects.length))
  def isClosed: Boolean = closed
  def close(): Unit =
    if !closed then
      closed = true
      indexed = None
      file.close()

object PdfPacketSource:
  final case class Stats(windowBytes: Int, windowLoads: Long, externalPayloadBytes: Long, objects: Int)
  def open(path: Path, bounds: PdfPacket.Bounds = PdfPacket.Bounds()): PdfPacketSource < (Abort[PdfError] & Sync) =
    if !PdfPacket.validBounds(bounds) then Abort.fail(PdfError.InvalidPdf("Invalid packet bounds"))
    else Abort.catching[IOException](_ => PdfError.InvalidPdf("Packet source open failed")) {
      Sync.defer(new RandomAccessFile(path.toFile, "r")).map { file =>
        Abort.catching[IOException] { _ => file.close(); PdfError.InvalidPdf("Packet source read failed") } {
          Sync.defer(file.length()).map { size =>
            if size > bounds.sourceBytes then
              Sync.defer(file.close()).map(_ => Abort.fail(PdfError.TooLarge(bounds.sourceBytes, size)))
            else
              val source = new PdfPacketSource(file, size, bounds)
              Sync.defer(source.index()).map {
                case Right(_) => source
                case Left(error) => Sync.defer(source.close()).map(_ => Abort.fail(error))
              }
          }
        }
      }
    }
