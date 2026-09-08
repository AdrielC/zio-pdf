# zio-pdf × TACIT — agent pipeline DSL

Use [TACIT](https://github.com/lampepfl/tacit) so agents **compose PDF pipelines in Scala** with capability tracking, in a style similar to [Kyo Flow](https://getkyo.io/latest/kyo-flow/) and volga SMC wiring.

## Two styles agents can write

### 1. Kyo Flow style (recommended for TACIT)

Fluent named steps, pure plan, then run:

```scala
import zio.pdf.arrow.*
import zio.pdf.pipe.*

val ingest = PipelineFlow
  .init("court-ingest")
  .input[Array[Byte]]("bytes")
  .pipe("slice")(DecodePipeline.sliceWhole)
  .pipe("hyperfuse-decode-digest") {
    Pipe(slice => IngestPipeline.fusedDecodeAndDigest(slice, FusedDecode.Cfg()))
  }
  .build

println(ingest.renderMermaid)   // inspect wiring (pure)
val result = ingest.runLocal(bytes)
```

Also: `.output(...)`, `.step(...)`, `.andThen(...)`, `.zip(...)`, `.dispatch(...).when(...).otherwise(...)`.

Canonical PDF recipes: `PdfFlow.ingestFused()` / `PdfFlow.ingestStaged()`.

### 2. Volga SMC wiring (port graphs)

```scala
import volga.free.Nat
import volga.syntax.smc.V
type V1 = V[Nat.`1`]

val diagram = PipelineFlow.wiring[Nat.`1`, Nat.`0`]("ingest", Tuple1("bytes")) { prop =>
  val slice = ArrowSyntax.node("slice", 1, 1)
  val fuse  = ArrowSyntax.node("fuse", 1, 0)
  prop.of1((v: V1) => fuse(slice(v)))
}
println(diagram.mermaid)
```

## TACIT REPL example

See `agent-script.scala` — nested `requestFileSystem` + `requestPdfPipeline`.

## Wiring into TACIT

1. Publish or `publishLocal` zio-pdf; add dependency per `build.sbt.snippet`
2. Copy `library/PdfPipelineOps.scala` → `tacit/library/impl/`
3. Paste `library/InterfaceAdditions.scala` into `tacit/library/Interface.scala`
4. In `InterfaceImpl.scala`: `export PdfPipelineOps.*` and implement `requestPdfPipeline`
5. Add `code-validator.snippet` patterns to block `PdfHyperdrive` / `PdfEngine` bypass
6. Rebuild: `./build.sh`

### Capability nesting

- **`requestFileSystem`** — where bytes may be read
- **`requestPdfPipeline`** — permission to inspect/run ingest graphs

Pure graph inspection (`ingest.renderMermaid`, `pdfPlanFlow(flow)`) only needs `PdfPipelineCapability`.

## vs Kyo Flow

| Kyo Flow | zio-pdf `PipelineFlow` |
|----------|------------------------|
| `Flow.init("x").output("y")(...)` | `.init("x").input(...).output("y")(...).build` |
| `Flow.runLocal(flow, inputs)` | `flow.runLocal(input)` |
| `flow.andThen(other)` | `flow.andThen(other)` |
| `Flow.renderMermaid(flow)` | `flow.renderMermaid` |
| Durable execution engine | Pure plans + local run; TACIT scopes effects |

Full Kyo Record-typed context (`ctx.price`) is not replicated — use tuple carriers or chain `.output` steps that thread one type. Named fields appear in **graph labels** and **ScanGraph** either way.
