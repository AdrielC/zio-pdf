/*
 * Triple-fused hyperdrive: streaming parse → ObjStm/XRef expansion → element
 * classification in one pass. Never materialises [[StreamingDecoded]] or
 * [[Decoded]] timelines.
 */

package zio.pdf.pipe

import zio.Chunk
import zio.pdf.Element
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

private[pdf] object FusedElements {

  def decodeSlice(slice: Slice, cfg: Cfg): Chunk[Element] = {
    val builder = Chunk.newBuilder[Element]
    HyperFuse.fuseElementsBuild(slice, cfg, el => builder += el)
    builder.result()
  }

  /**
   * Sink each [[Element]] as produced — never materialises timelines.
   * `sink` is `inline` so call-site lambdas fuse into the HyperFuse loop.
   */
  inline def decodeSliceSink(slice: Slice, cfg: Cfg)(inline sink: Element => Unit): Long = {
    var count = 0L
    HyperFuse.fuseElementsBuild(slice, cfg, el => { sink(el); count += 1 })
    count
  }

  def decodeBytes(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: zio.pdf.StreamingDecode.Config = zio.pdf.StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    decodeSlice(Slice(bytes, 0, bytes.length), Cfg(enableDiagnostics, config, batchSize))

  val decode: Pipe[(Slice, Cfg), Chunk[Element]] =
    Pipe { case (slice, cfg) => decodeSlice(slice, cfg) }
}
