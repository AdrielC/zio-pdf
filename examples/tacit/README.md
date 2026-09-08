# zio-pdf × TACIT

Use [TACIT](https://github.com/lampepfl/tacit) (Tracked Agent Capabilities In Types) so agents **compose and inspect PDF ingest pipelines in Scala**, with wiring tracked in types and execution scoped to granted capabilities.

## Mental model

| TACIT concept | zio-pdf mapping |
|---------------|-----------------|
| Capability scope (`requestFileSystem`) | Read PDF bytes from allowed paths only |
| Pure inspection | `PdfPipeline.planFused()` → `PipelinePlan` (mermaid, edges, `schemaJson`) |
| Effectful run | `PdfPipeline.runFused(bytes)` after reading file |
| Agent-composed graph | `PipelineGraph.node(...)` + `>>>` + `PipelineGraph.analyze` |

Volga SMC `prop.ofN { ... }` macros are for **human/compile-time** wiring in zio-pdf tests. In TACIT safe mode, agents should use:

1. **Prebuilt plans** — `PdfPipeline.planFused` / `planStaged`
2. **Labeled `FreeArrow` composition** — no macros, pure analyze, then run via `PipelineGraph.run`

## Quick agent example (TACIT REPL)

After extending the TACIT library (below):

```scala
import language.experimental.safe
import tacit.library.*
import zio.pdf.tacit.PdfPipeline
import zio.pdf.arrow.*

requestFileSystem("/project") {
  // 1. Pure: inspect the ingest wiring before running anything
  val plan = PdfPipeline.planFused()
  println(plan.mermaid)
  println(plan.edges)

  // 2. Effect: read PDF under the granted root, run fused HyperFuse ingest
  val bytes = access("samples/xref-stream.pdf").readBytes()
  val summary = PdfPipeline.runFused(bytes)
  println(s"decoded=${summary.decodedCount} digest=${summary.digestHex.take(16)}...")
}
```

## Extending TACIT

1. Publish or `publishLocal` zio-pdf; add dependency per `build.sbt.snippet`
2. Copy `examples/tacit/library/PdfPipelineOps.scala` → `tacit/library/impl/`
3. Paste `library/InterfaceAdditions.scala` into `tacit/library/Interface.scala`
4. In `InterfaceImpl.scala`: `export PdfPipelineOps.*` and implement `requestPdfPipeline`
5. Add `code-validator.snippet` patterns to block `PdfHyperdrive` / `PdfEngine` bypass
6. Rebuild: `./build.sh`
7. Try `agent-script.scala` in the TACIT REPL

### Capability nesting

`requestPdfPipeline` is intentionally separate from `requestFileSystem`:

- **Filesystem** — where bytes may be read (`access(...).readBytes()`)
- **PdfPipeline** — permission to inspect wiring and run ingest graphs

Agents must hold both to read a PDF and ingest it. Pure graph inspection (`pdfPlanFused`) only needs `PdfPipelineCapability`.

See `library/InterfaceAdditions.scala` for the capability trait additions.

## Why not Dagon?

TACIT + volga/`ScanGraph` give **tracked graphs in types**. [Dagon](https://github.com/stripe/dagon/) is for **rewriting** DAGs (fusion rules). Add Dagon later if agents should optimize pipelines before execution.
