/*
 * Ingest graphs — volga parallel blocks over one byte source.
 *
 * Decode + digest share one [[HyperFuse]] scan (no second pass over bytes).
 */

package zio.pdf.pipe

import java.nio.file.Path

import zio.Chunk
import zio.pdf.{Decoded, Element}
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}
import zio.pdf.pipe.HyperFuse.DigestSink

object IngestPipeline {

  final case class DecodeDigest[A](decoded: A, digest: Array[Byte])

  /** Fused decode + SHA-256 on the same batch windows. */
  def fusedDecodeAndDigest(slice: Slice, cfg: Cfg): DecodeDigest[Chunk[Decoded]] = {
    val (decoded, digest) = HyperFuse.fuseDecodedWithDigest(slice, cfg)
    DecodeDigest(decoded, digest)
  }

  /** Sink each [[Decoded]] + digest raw bytes — no timeline [[Chunk]]. */
  def fusedDecodeAndDigestSink(slice: Slice, cfg: Cfg)(sink: Decoded => Unit): DigestSink =
    HyperFuse.fuseDecodedWithDigestSink(slice, cfg)(sink)

  def fusedElementsAndDigest(slice: Slice, cfg: Cfg): DecodeDigest[Chunk[Element]] = {
    val (elements, digest) = HyperFuse.fuseElementsWithDigest(slice, cfg)
    DecodeDigest(elements, digest)
  }

  def fusedElementsAndDigestSink(slice: Slice, cfg: Cfg)(sink: Element => Unit): DigestSink =
    HyperFuse.fuseElementsWithDigestSink(slice, cfg)(sink)

  def fromSlice[A](cfg: Cfg, fuse: (Slice, Cfg) => DecodeDigest[A]): Pipe[Path, DecodeDigest[A]] =
    DecodePipeline.readSlice(cfg) >>> Pipe(slice => fuse(slice, cfg))

  def fromPathSink(cfg: Cfg = Cfg()): (Path, Decoded => Unit) => DigestSink =
    (path, sink) => fusedDecodeAndDigestSink(DecodePipeline.readSlice(cfg).run(path), cfg)(sink)

  def elementsFromPathSink(cfg: Cfg = Cfg()): (Path, Element => Unit) => DigestSink =
    (path, sink) => fusedElementsAndDigestSink(DecodePipeline.readSlice(cfg).run(path), cfg)(sink)

  object decodeAndDigest {
    def fromBytes(cfg: Cfg = Cfg()): Pipe[Array[Byte], DecodeDigest[Chunk[Decoded]]] =
      DecodePipeline.sliceWhole >>> Pipe(slice => fusedDecodeAndDigest(slice, cfg))

    def fromBytesSink(cfg: Cfg = Cfg()): (Array[Byte], Decoded => Unit) => DigestSink =
      (bytes, sink) => fusedDecodeAndDigestSink(FusedDecode.Slice(bytes, 0, bytes.length), cfg)(sink)

    def fromPathMmap(cfg: Cfg = Cfg()): Pipe[Path, DecodeDigest[Chunk[Decoded]]] =
      DecodePipeline.readSliceMmap >>> Pipe(slice => fusedDecodeAndDigest(slice, cfg))

    def fromPathUring(cfg: Cfg = Cfg()): Pipe[Path, DecodeDigest[Chunk[Decoded]]] =
      fromPathMmap(cfg)

    def fromPath(cfg: Cfg = Cfg()): Pipe[Path, DecodeDigest[Chunk[Decoded]]] =
      fromSlice(cfg, fusedDecodeAndDigest)

    def fromPathSink(cfg: Cfg = Cfg()): (Path, Decoded => Unit) => DigestSink =
      IngestPipeline.fromPathSink(cfg)

    def elementsFromBytes(cfg: Cfg = Cfg()): Pipe[Array[Byte], DecodeDigest[Chunk[Element]]] =
      DecodePipeline.sliceWhole >>> Pipe(slice => fusedElementsAndDigest(slice, cfg))

    def elementsFromPath(cfg: Cfg = Cfg()): Pipe[Path, DecodeDigest[Chunk[Element]]] =
      fromSlice(cfg, fusedElementsAndDigest)

    def elementsFromPathSink(cfg: Cfg = Cfg()): (Path, Element => Unit) => DigestSink =
      IngestPipeline.elementsFromPathSink(cfg)
  }
}
