/*
 * Non-JMH head-to-head for the Blocks scanner lift. Prints timings; does
 * not fail on speed. Correctness is the object-count match against step().
 *
 *   sbt 'root/testOnly zio.pdf.ScannerLiftPerfBench'
 */

package zio.pdf

import java.nio.file.Files
import java.nio.file.Path

import zio.*
import zio.blocks.chunk.{Chunk as BlocksChunk}
import zio.blocks.streams.Stream as BlocksStream
import zio.blocks.streams.io.Reader
import zio.stream.ZStream
import zio.test.*

object ScannerLiftPerfBench extends ZIOSpecDefault {

  private val fixture = Path.of("src/test/resources/court-corpus/oknd-general-order-2024-09.pdf")
  private val window  = 64 * 1024

  private def load(): Chunk[Byte] =
    Chunk.fromArray(Files.readAllBytes(fixture))

  private def asError(error: Throwable): PdfObjectScanner.Error =
    error match {
      case err: PdfObjectScanner.Error => err
      case other =>
        val detail = Option(other.getMessage).filter(_.nonEmpty).getOrElse(other.getClass.getSimpleName)
        PdfObjectScanner.Error.Malformed(detail, other)
    }

  /** One ZIO per byte — the lift the rewrite was meant to kill. Uses `readByte`
    * so a `0xFF` payload byte cannot collide with a sentinel.
    */
  private def perByteStream(bytes: Chunk[Byte]): ZStream[Any, Throwable, Byte] = {
    val reader = Reader.fromChunk(BlocksLift.toBlocksChunk(bytes))
    ZStream.repeatZIOOption {
      ZIO
        .attempt {
          val n = reader.readByte()
          if n < 0 then None else Some(n.toByte)
        }
        .foldZIO(
          error => ZIO.fail(Some(error)),
          {
            case None        => ZIO.fail(None)
            case Some(value) => ZIO.succeed(value)
          }
        )
    }
  }

  private def run[A](effect: ZIO[Any, Any, A]): A =
    Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe.run(effect).getOrThrowFiberFailure()
    }

  private def timeNanos(label: String, warmup: Int, repeats: Int)(thunk: => Int): Long = {
    var i = 0
    while i < warmup do
      val _ = thunk
      i += 1
    val t0 = java.lang.System.nanoTime()
    var j  = 0
    var last = 0
    while j < repeats do
      last = thunk
      j += 1
    val ns = (java.lang.System.nanoTime() - t0) / repeats.toLong
    println(f"  [$label%-52s] ${ns / 1000.0}%9.1f µs / iter  (n=$last, avg of $repeats)")
    ns
  }

  def spec: Spec[Any, Any] = suite("ScannerLiftPerfBench")(
    test("court PDF: tight scan vs ZIO lifts") {
      val bytes    = load()
      val expected = PdfObjectScanner.step(PdfObjectScanner.Config.default, PdfObjectScanner.initial, bytes)
      val nExpect  = expected.map(_._2.length).getOrElse(-1)
      val warmup   = 8
      val repeats  = 40

      println(
        s"\n=== PdfObjectScanner lift (${bytes.length} bytes, $nExpect objects, lower is better) ==="
      )

      val stepNs = timeNanos("step(whole Chunk)  [decode baseline]", warmup, repeats) {
        PdfObjectScanner.step(PdfObjectScanner.Config.default, PdfObjectScanner.initial, bytes) match {
          case Right((_, found)) => found.length
          case Left(err)         => throw err
        }
      }

      val scanNs = timeNanos("scan(Reader[Byte])  [tight readBytes]", warmup, repeats) {
        val reader = Reader.fromChunk(BlocksLift.toBlocksChunk(bytes))
        PdfObjectScanner.scan(reader) match {
          case Right(found) => found.length
          case Left(err)    => throw err
        }
      }

      val sinkNs = timeNanos("scan(Blocks Stream)  [Sink.create]", warmup, repeats) {
        val source = BlocksStream.fromChunk(BlocksLift.toBlocksChunk(bytes))
        PdfObjectScanner.scan(source) match {
          case Right(found) => found.length
          case Left(err)    => throw err
        }
      }

      val windowsNs = timeNanos("streamWindows(Reader[Byte])  [1 ZIO / 64KiB]", warmup, repeats) {
        val reader = Reader.fromChunk(BlocksLift.toBlocksChunk(bytes))
        run(PdfObjectScanner.streamWindows(reader).runCount).toInt
      }

      val streamNs = timeNanos("stream(Reader[Byte]).runCollect  [flatten]", warmup, repeats) {
        val reader = Reader.fromChunk(BlocksLift.toBlocksChunk(bytes))
        run(PdfObjectScanner.stream(reader).runCollect).length
      }

      val zstreamNs = timeNanos("stream(ZStream.rechunk 64KiB)  [old window ZIO]", warmup, repeats) {
        val windows = ZStream.fromChunk(bytes).rechunk(window).chunks.mapError(asError)
        run(PdfObjectScanner.stream(windows).runCollect).length
      }

      val oldWindowNs = timeNanos("fromReader(window)+stream  [old lift]", warmup, repeats) {
        val windows = BlocksChunk(BlocksLift.toBlocksChunk(bytes))
        val lifted =
          BlocksLift
            .fromReader(Reader.fromChunk(windows), null)
            .mapError(asError)
            .map(BlocksLift.toZioChunk)
        run(PdfObjectScanner.stream(lifted).runCollect).length
      }

      val perByteNs = timeNanos("ZIO.attempt(readByte)  [1 ZIO / byte]", 2, 4) {
        run(perByteStream(bytes).runCollect).length
      }

      val perByteScanNs = timeNanos("readByte ZStream + stream  [ZIO / byte]", 2, 4) {
        val windows = perByteStream(bytes).mapError(asError).rechunk(window).chunks
        run(PdfObjectScanner.stream(windows).runCollect).length
      }

      def us(ns: Long): String = f"${ns / 1000.0}%.1f"

      println(
        s"  step=${us(stepNs)}µs  scan=${us(scanNs)}µs  sink=${us(sinkNs)}µs  " +
          s"windows=${us(windowsNs)}µs  flatten=${us(streamNs)}µs  " +
          s"zstream=${us(zstreamNs)}µs  oldLift=${us(oldWindowNs)}µs  " +
          s"perByte=${us(perByteNs)}µs  perByteScan=${us(perByteScanNs)}µs"
      )

      val scanned = PdfObjectScanner.scan(Reader.fromChunk(BlocksLift.toBlocksChunk(bytes)))
      assertTrue(
        expected.isRight,
        scanned.exists(_.length == nExpect),
        nExpect > 0
      )
    } @@ TestAspect.withLiveClock @@ TestAspect.timeout(3.minutes)
  ) @@ TestAspect.sequential
}
