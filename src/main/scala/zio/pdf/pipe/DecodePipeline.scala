/*
 * Composed hyperdrive pipelines — volga / [[Pipe]] arrows as plain fused functions.
 *
 * Sequential stages use `>>>`; fan-out of independent views uses `<>` / `&&&`
 * (see [[PipeCat]] for the full CartesianCat instance).
 */

package zio.pdf.pipe

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}

import zio.Chunk
import zio.pdf.{Decoded, Element, Elements, StreamingDecoded}
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

object DecodePipeline {

  val sliceWhole: Pipe[Array[Byte], Slice] =
    Pipe(bytes => Slice(bytes, 0, bytes.length))

  val decodeFused: Pipe[(Slice, Cfg), Chunk[Decoded]] = FusedDecode.decode

  val decodeStreaming: Pipe[(Slice, Cfg), Chunk[StreamingDecoded]] =
    Pipe { case (slice, cfg) => FusedDecode.decodeStreamingSlice(slice, cfg) }

  val classifyElements: Pipe[Chunk[Decoded], Chunk[Element]] =
    Pipe(Elements.foldSync)

  def fromBytes(cfg: Cfg = Cfg()): Pipe[Array[Byte], Chunk[Decoded]] =
    sliceWhole >>> Pipe(slice => FusedDecode.decodeSlice(slice, cfg))

  val mmapRead: Pipe[Path, MappedByteBuffer] =
    Pipe { path =>
      val channel = FileChannel.open(path, StandardOpenOption.READ)
      try {
        val size = channel.size()
        require(size <= Int.MaxValue, s"file too large for hyperdrive mmap: $size bytes")
        channel.map(FileChannel.MapMode.READ_ONLY, 0L, size)
      } finally
        channel.close()
    }

  private def sliceFromMapped(mapped: MappedByteBuffer): Slice =
    if mapped.hasArray then
      Slice(
        mapped.array(),
        mapped.arrayOffset() + mapped.position(),
        mapped.remaining()
      )
    else
      val dup = mapped.duplicate()
      val arr = new Array[Byte](dup.remaining())
      dup.get(arr)
      Slice(arr, 0, arr.length)

  val bytesFromMapped: Pipe[MappedByteBuffer, Slice] = Pipe(sliceFromMapped)

  val readSliceMmap: Pipe[Path, Slice] =
    mmapRead >>> bytesFromMapped

  def readSlice(cfg: Cfg = Cfg()): Pipe[Path, Slice] = readSliceMmap

  def decodeSlice(cfg: Cfg = Cfg()): Pipe[Slice, Chunk[Decoded]] =
    Pipe(slice => FusedDecode.decodeSlice(slice, cfg))

  def decodeSliceSink(cfg: Cfg = Cfg()): (Slice, Decoded => Unit) => Long =
    (slice, sink) => FusedDecode.decodeSliceSink(slice, cfg)(sink)

  def elementsSlice(cfg: Cfg = Cfg()): Pipe[Slice, Chunk[Element]] =
    Pipe(slice => FusedElements.decodeSlice(slice, cfg))

  def elementsSliceSink(cfg: Cfg = Cfg()): (Slice, Element => Unit) => Long =
    (slice, sink) => FusedElements.decodeSliceSink(slice, cfg)(sink)

  def fromPathMmap(cfg: Cfg = Cfg()): Pipe[Path, Chunk[Decoded]] =
    readSliceMmap >>> decodeSlice(cfg)

  def fromPathSink(cfg: Cfg = Cfg()): (Path, Decoded => Unit) => Long =
    (path, sink) => decodeSliceSink(cfg)(readSlice(cfg).run(path), sink)

  def fromPathMmapSink(cfg: Cfg = Cfg()): (Path, Decoded => Unit) => Long =
    (path, sink) => decodeSliceSink(cfg)(readSliceMmap.run(path), sink)

  def elementsFromPathSink(cfg: Cfg = Cfg()): (Path, Element => Unit) => Long =
    (path, sink) => elementsSliceSink(cfg)(readSlice(cfg).run(path), sink)

  def fromPathUring(cfg: Cfg = Cfg()): Pipe[Path, Chunk[Decoded]] =
    fromPathMmap(cfg)

  def fromPath(cfg: Cfg = Cfg()): Pipe[Path, Chunk[Decoded]] =
    readSlice(cfg) >>> decodeSlice(cfg)

  def elementsFromBytes(cfg: Cfg = Cfg()): Pipe[Array[Byte], Chunk[Element]] =
    sliceWhole >>> elementsSlice(cfg)

  def elementsFromPathMmap(cfg: Cfg = Cfg()): Pipe[Path, Chunk[Element]] =
    readSliceMmap >>> elementsSlice(cfg)

  def elementsFromPath(cfg: Cfg = Cfg()): Pipe[Path, Chunk[Element]] =
    readSlice(cfg) >>> elementsSlice(cfg)

  def elementsFromPathUring(cfg: Cfg = Cfg()): Pipe[Path, Chunk[Element]] =
    elementsFromPathMmap(cfg)

  /** Two-phase elements (decode timeline then classify) — parity / debug only. */
  def elementsStagedFromBytes(cfg: Cfg = Cfg()): Pipe[Array[Byte], Chunk[Element]] =
    fromBytes(cfg) >>> classifyElements
}
