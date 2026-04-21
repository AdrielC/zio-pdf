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

import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import _root_.cats.effect.IO
import _root_.cats.effect.unsafe.implicits.global as ceGlobal
import _root_.fs2.{Chunk as FsChunk, Stream as FsStream}
import _root_.fs2.interop.scodec.{StreamDecoder as FsStreamDecoder}

import _root_.zio.{Chunk, Runtime, Unsafe}
import _root_.zio.stream.ZStream
import _root_.zio.pdf.TopLevel
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

  // The shared scodec.Decoder[TopLevel] - both libraries see this.
  private val tlDecoder = TopLevel.streamDecoder

  // Pre-built StreamDecoders.
  private val zioMany = TopLevel.streamDecoder.streamMany
  private val fsMany  = FsStreamDecoder.many(tlDecoder)

  @Setup(Level.Trial)
  def setup(): Unit = {
    // The fixture is on the resources classpath; load via the
    // classloader so we don't depend on the bench's CWD.
    val is  = getClass.getResourceAsStream("/xref-stream.pdf")
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

  @Benchmark
  def zio_decode_pdf_topLevel: Long = {
    val source = ZStream
      .fromChunk(Chunk.fromArray(bytes))
      .grouped(chunkSize)
      .map(c => _root_.scodec.bits.BitVector.view(c.toArray))
    Unsafe.unsafe { implicit u =>
      zioRuntime.unsafe.run(source.viaDecoder(zioMany).runCount).getOrThrow()
    }
  }

  @Benchmark
  def zio_decode_pdf_topLevel_byteStream: Long = {
    // Mirrors the real PdfStream.topLevel call shape: byte stream
    // straight in, no manual rechunking.
    val source: ZStream[Any, Throwable, Byte] = ZStream.fromChunk(Chunk.fromArray(bytes))
    Unsafe.unsafe { implicit u =>
      zioRuntime.unsafe.run(source.via(TopLevel.pipe).runCount).getOrThrow()
    }
  }
}
