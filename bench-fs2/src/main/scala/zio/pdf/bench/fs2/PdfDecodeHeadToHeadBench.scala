/*
 * Real-PDF head-to-head: parse the legacy `xref-stream.pdf`
 * fixture's top-level structure (versions / comments / objects /
 * xrefs / startxrefs) using both libraries' StreamDecoder over
 * the same `scodec.Decoder[zio.pdf.TopLevel]`.
 *
 * Run with:
 *
 *   sbt 'benchFs2/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu us .*PdfDecodeHeadToHeadBench.*'
 */

package zio.pdf.bench.fs2

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import _root_.cats.effect.IO
import _root_.cats.effect.unsafe.implicits.global as ceGlobal
import _root_.fs2.{Chunk as FsChunk, Stream as FsStream}
import _root_.fs2.interop.scodec.{StreamDecoder as FsStreamDecoder}

import _root_.zio.{Chunk, Runtime, Unsafe}
import _root_.zio.stream.ZStream
import _root_.zio.pdf.{PdfStream, TopLevel}
import _root_.zio.scodec.stream.syntax.*

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class PdfDecodeHeadToHeadBench {

  /** Upstream chunk size before TopLevel decoding. */
  @Param(Array("8192"))
  var chunkSize: Int = uninitialized

  private var bytes: Array[Byte] = uninitialized
  private val zioRuntime          = Runtime.default

  private val tlDecoder = TopLevel.streamDecoder
  private val zioMany   = TopLevel.streamDecoder.streamMany
  private val fsMany    = FsStreamDecoder.many(tlDecoder)

  @Setup(Level.Trial)
  def setup(): Unit = {
    val is = getClass.getResourceAsStream("/xref-stream.pdf")
    require(is != null, "xref-stream.pdf not on classpath")
    val baos = new java.io.ByteArrayOutputStream()
    val buf  = new Array[Byte](8192)
    var n    = is.read(buf)
    while (n >= 0) { baos.write(buf, 0, n); n = is.read(buf) }
    is.close()
    bytes = baos.toByteArray
  }

  @Benchmark
  def fs2_decode_pdf_topLevel: Long = {
    val source = FsStream
      .chunk(FsChunk.array(bytes))
      .covary[IO]
      .chunkN(chunkSize)
      .map(c => _root_.scodec.bits.BitVector.view(c.toArray))
    source.through(fsMany.toPipe).compile.count.unsafeRunSync()
  }

  /**
   * Apples-to-apples with fs2: same micro-chunk size, but stay on
   * bytes (zero-copy [[ChunkBytes]] inside [[StreamDecoder.toBytePipeline]]).
   */
  @Benchmark
  def zio_decode_pdf_topLevel: Long = {
    val source = ZStream.fromChunk(Chunk.fromArray(bytes)).rechunk(chunkSize)
    Unsafe.unsafe { implicit u =>
      zioRuntime.unsafe.run(source.via(zioMany.toBytePipeline).runCount).getOrThrow()
    }
  }

  /** Production shape: byte stream + [[TopLevel.pipe]] (rechunk + zero-copy). */
  @Benchmark
  def zio_decode_pdf_topLevel_byteStream: Long = {
    val source: ZStream[Any, Throwable, Byte] = ZStream.fromChunk(Chunk.fromArray(bytes))
    Unsafe.unsafe { implicit u =>
      zioRuntime.unsafe.run(source.via(TopLevel.pipe).runCount).getOrThrow()
    }
  }

  /** Production facade: [[PdfStream.topLevel]]. */
  @Benchmark
  def zio_decode_pdf_topLevel_pdfStream: Long = {
    val source: ZStream[Any, Throwable, Byte] = ZStream.fromChunk(Chunk.fromArray(bytes))
    Unsafe.unsafe { implicit u =>
      zioRuntime.unsafe.run(source.via(PdfStream.topLevel).runCount).getOrThrow()
    }
  }

  /** Strict in-memory: one `runStrict`, no `Runtime`, no `ZChannel`. */
  @Benchmark
  def zio_decode_pdf_topLevel_strict: Int =
    TopLevel.decodeAll(bytes).fold(_ => 0, _.size)
}
