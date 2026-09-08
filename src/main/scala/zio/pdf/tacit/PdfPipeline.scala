package zio.pdf.tacit

import zio.Chunk
import zio.blocks.schema.{DynamicValue, Schema}
import zio.pdf.Decoded
import zio.pdf.arrow.*
import zio.pdf.pipe.FusedDecode.Cfg

/**
 * Tacit/agent-facing pipeline views — durable [[ScanGraph]] plans plus fused
 * ingest execution. Designed for [TACIT](https://github.com/lampepfl/tacit):
 * agents inspect `PipelinePlan` (pure) inside capability scopes, then run ingest
 * on bytes read via `requestFileSystem`.
 */
object PdfPipeline {

  /** Serializable, inspectable pipeline description (diagrams + durable [[ScanGraph]] JSON). */
  final case class PipelinePlan(
      name:       String,
      mermaid:    String,
      dot:        String,
      edges:      Vector[(String, String)],
      nodes:      Vector[String],
      schemaJson: String
  ) derives Schema, CanEqual

  /** Result summary safe to log (no raw PDF bytes). */
  final case class IngestSummary(
      decodedCount: Int,
      digestHex:    String
  ) derives Schema, CanEqual

  def planFused(name: String = "ingest-fused", cfg: Cfg = Cfg()): PipelinePlan =
    planFromSchema(name, IngestGraph.schemaFused(cfg))

  def planStaged(name: String = "ingest-staged", cfg: Cfg = Cfg()): PipelinePlan =
    planFromSchema(name, IngestGraph.schemaStaged(cfg))

  def planFromSchema(name: String, schema: ScanGraph, inputLabel: String = "bytes"): PipelinePlan = {
    val wiring = PipelineGraph.schemaToWiring(schema, inputLabel)
    PipelinePlan(
      name       = name,
      mermaid    = GraphRender.mermaid(name, wiring),
      dot        = GraphRender.dot(name, wiring),
      edges      = wiring._2,
      nodes      = ScanGraph.nodeNames(schema),
      schemaJson = schemaToJson(schema)
    )
  }

  def runFused(bytes: Array[Byte], cfg: Cfg = Cfg()): IngestSummary = {
    val result = IngestGraph.runFused(cfg).run(bytes)
    summarize(result.decoded, result.digest)
  }

  def runStaged(bytes: Array[Byte], cfg: Cfg = Cfg()): IngestSummary = {
    val result = IngestGraph.runStaged(cfg).run(bytes)
    summarize(result.decoded, result.digest)
  }

  /** Pure: analyze a labeled graph the agent composed with [[PipelineGraph]]. */
  def planFromGraph(name: String, graph: FreeArrow[PipelineGraph.Node, ?, ?]): PipelinePlan =
    planFromSchema(name, PipelineGraph.analyze(graph))

  /** Pure: inspect a [[PipelineFlow.Flow]] — serializable plan, no execution. */
  def planFromFlow[A, B](flow: PipelineFlow.Flow[A, B]): PipelinePlan =
    planFromGraph(flow.name, flow.graph)

  /** Round-trip [[ScanGraph]] through zio-blocks [[Schema]] (durable storage). */
  def schemaRoundTrip(schema: ScanGraph): Either[String, ScanGraph] =
    summon[Schema[ScanGraph]].fromDynamicValue(summon[Schema[ScanGraph]].toDynamicValue(schema)).left.map(_.toString)

  private def summarize(decoded: Chunk[Decoded], digest: Array[Byte]): IngestSummary =
    IngestSummary(
      decodedCount = decoded.size,
      digestHex    = digest.map(b => f"$b%02x").mkString
    )

  private def schemaToJson(schema: ScanGraph): String = {
    val dv = summon[Schema[ScanGraph]].toDynamicValue(schema)
    dynamicValueToJson(dv)
  }

  private def dynamicValueToJson(dv: DynamicValue): String =
    dv match {
      case DynamicValue.Primitive(v) =>
        v match {
          case zio.blocks.schema.PrimitiveValue.String(s) => "\"" + s.replace("\"", "\\\"") + "\""
          case zio.blocks.schema.PrimitiveValue.Int(i)     => i.toString
          case zio.blocks.schema.PrimitiveValue.Long(l)    => l.toString
          case zio.blocks.schema.PrimitiveValue.Boolean(b) => b.toString
          case other                                       => other.toString
        }
      case DynamicValue.Sequence(elems) =>
        elems.map(dynamicValueToJson).mkString("[", ",", "]")
      case DynamicValue.Record(fields) =>
        fields.map { case (k, v) => s"\"$k\":${dynamicValueToJson(v)}" }.mkString("{", ",", "}")
      case DynamicValue.Variant(caseName, value) =>
        s"""{"case":"$caseName","value":${dynamicValueToJson(value)}}"""
      case DynamicValue.Map(entries) =>
        entries.map { case (k, v) => s"${dynamicValueToJson(k)}:${dynamicValueToJson(v)}" }.mkString("{", ",", "}")
      case DynamicValue.Null => "null"
    }
}
