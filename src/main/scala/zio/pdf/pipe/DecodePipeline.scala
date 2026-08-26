/*
 * Composed in-memory decode pipelines — volga / [[Pipe]] arrows as plain
 * fused functions. File paths are driven incrementally by [[FusedDecoder]].
 *
 * Sequential stages use `>>>`; fan-out of independent views uses `<>` / `&&&`
 * (see [[PipeCat]] for the full CartesianCat instance).
 */

package zio.pdf.pipe

import zio.Chunk
import zio.pdf.{Decoded, Element, Elements, StreamingDecoded}
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

private[pdf] object DecodePipeline {

  val sliceWhole: Pipe[Array[Byte], Slice] =
    Pipe(bytes => Slice(bytes, 0, bytes.length))

  val decodeFused: Pipe[(Slice, Cfg), Chunk[Decoded]] = FusedDecode.decode

  val decodeStreaming: Pipe[(Slice, Cfg), Chunk[StreamingDecoded]] =
    Pipe { case (slice, cfg) => FusedDecode.decodeStreamingSlice(slice, cfg) }

  val classifyElements: Pipe[Chunk[Decoded], Chunk[Element]] =
    Pipe(Elements.foldSync)

  def fromBytes(cfg: Cfg = Cfg()): Pipe[Array[Byte], Chunk[Decoded]] =
    sliceWhole >>> Pipe(slice => FusedDecode.decodeSlice(slice, cfg))

  def decodeSlice(cfg: Cfg = Cfg()): Pipe[Slice, Chunk[Decoded]] =
    Pipe(slice => FusedDecode.decodeSlice(slice, cfg))

  def decodeSliceSink(cfg: Cfg = Cfg()): (Slice, Decoded => Unit) => Long =
    (slice, sink) => FusedDecode.decodeSliceSink(slice, cfg)(sink)

  def elementsSlice(cfg: Cfg = Cfg()): Pipe[Slice, Chunk[Element]] =
    Pipe(slice => FusedElements.decodeSlice(slice, cfg))

  def elementsSliceSink(cfg: Cfg = Cfg()): (Slice, Element => Unit) => Long =
    (slice, sink) => FusedElements.decodeSliceSink(slice, cfg)(sink)

  def elementsFromBytes(cfg: Cfg = Cfg()): Pipe[Array[Byte], Chunk[Element]] =
    sliceWhole >>> elementsSlice(cfg)

  /** Two-phase elements (decode timeline then classify) — parity / debug only. */
  def elementsStagedFromBytes(cfg: Cfg = Cfg()): Pipe[Array[Byte], Chunk[Element]] =
    fromBytes(cfg) >>> classifyElements
}
