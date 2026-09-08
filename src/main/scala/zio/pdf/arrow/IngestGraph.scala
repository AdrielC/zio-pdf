package zio.pdf.arrow

import zio.Chunk
import zio.pdf.{Decoded, Element}
import zio.pdf.pipe.FusedDecode.Cfg
import zio.pdf.pipe.{DecodePipeline, IngestPipeline, Pipe}

/** Ingest pipelines as labeled graphs — execution, volga wiring, and durable schema. */
object IngestGraph {

  type DecodeDigest[A] = IngestPipeline.DecodeDigest[A]

  /** Staged decode+digest (debug / parity) as a labeled [[FreeArrow]]. */
  def stagedDecoded(cfg: Cfg = Cfg()): FreeArrow[PipelineGraph.Node, Array[Byte], DecodeDigest[Chunk[Decoded]]] =
    PipelineGraph.node("slice", 1, 1)(DecodePipeline.sliceWhole) >>>
      PipelineGraph.node("staged-decode-digest", 1, 1)(IngestPipeline.stagedDecodeAndDigest(cfg))

  def runStaged(cfg: Cfg = Cfg()): Pipe[Array[Byte], DecodeDigest[Chunk[Decoded]]] =
    PipelineGraph.run(stagedDecoded(cfg))

  def runFused(cfg: Cfg = Cfg()): Pipe[Array[Byte], DecodeDigest[Chunk[Decoded]]] =
    PipelineGraph.run(fusedDecodedFromBytes(cfg))

  /** Production fused HyperFuse path: bytes → slice → single fused scan. */
  def fusedDecodedFromBytes(cfg: Cfg = Cfg()): FreeArrow[PipelineGraph.Node, Array[Byte], DecodeDigest[Chunk[Decoded]]] =
    PipelineGraph.node("slice", 1, 1)(DecodePipeline.sliceWhole) >>>
      PipelineGraph.node("hyperfuse-decode-digest", 1, 1) {
        Pipe(slice => IngestPipeline.fusedDecodeAndDigest(slice, cfg))
      }

  def fusedElementsFromBytes(cfg: Cfg = Cfg()): FreeArrow[PipelineGraph.Node, Array[Byte], DecodeDigest[Chunk[Element]]] =
    PipelineGraph.node("slice", 1, 1)(DecodePipeline.sliceWhole) >>>
      PipelineGraph.node("hyperfuse-elements-digest", 1, 1) {
        Pipe(slice => IngestPipeline.fusedElementsAndDigest(slice, cfg))
      }

  /** Durable schema from the staged labeled graph. */
  def schemaStaged(cfg: Cfg = Cfg()): ScanGraph =
    PipelineGraph.analyze(stagedDecoded(cfg))

  def schemaFused(cfg: Cfg = Cfg()): ScanGraph =
    PipelineGraph.analyze(fusedDecodedFromBytes(cfg))

  def renderStaged(title: String = "ingest-staged", cfg: Cfg = Cfg()): GraphFormats =
    PipelineGraph.renderGraph(title, stagedDecoded(cfg), "bytes")

  def renderFused(title: String = "ingest-fused", cfg: Cfg = Cfg()): GraphFormats =
    PipelineGraph.renderGraph(title, fusedDecodedFromBytes(cfg), "bytes")

  /** Edges extracted from analyzed schema (for tests / tooling). */
  def stagedWiring(cfg: Cfg = Cfg()): GraphRender.Wiring =
    PipelineGraph.schemaToWiring(schemaStaged(cfg), "bytes")

  def fusedWiring(cfg: Cfg = Cfg()): GraphRender.Wiring =
    PipelineGraph.schemaToWiring(schemaFused(cfg), "bytes")
}
