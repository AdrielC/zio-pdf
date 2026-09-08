package zio.pdf.tacit

import zio.Chunk
import zio.pdf.Decoded
import zio.pdf.pipe.*
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}
import zio.pdf.pipe.IngestPipeline.DecodeDigest

/** Public pipe building blocks for TACIT agents composing [[PipelineFlow]]. */
object PdfPipes {

  val sliceWhole: Pipe[Array[Byte], Slice] =
    DecodePipeline.sliceWhole

  def fusedDecodeAndDigest(cfg: Cfg = Cfg()): Pipe[Slice, DecodeDigest[Chunk[Decoded]]] =
    Pipe(slice => IngestPipeline.fusedDecodeAndDigest(slice, cfg))

  def stagedDecodeAndDigest(cfg: Cfg = Cfg()): Pipe[Slice, DecodeDigest[Chunk[Decoded]]] =
    IngestPipeline.stagedDecodeAndDigest(cfg)
}
