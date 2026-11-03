# Data-defined PDF pipelines

`kyo-pdf-pipeline` represents a PDF job as immutable, versioned data: named operations,
settings, input references and output references. The module depends on Kyo PDF;
it does not depend on ZIO, PDFBox, veraPDF, a JSON library, or Volga.

```scala
import io.github.adrielc.kyopdf.pipeline.{PdfPipeline, RecipeBuilder}

val build = new RecipeBuilder
val front = build.input("front")
val body = build.input("body")
val cut = build.select(body, 4, 1, 3)
val packet = build.assemble(Vector(front, cut), catalog = 1)
val marked = build.stamp(packet, "DRAFT")
val numbered = build.number(marked, start = 7, style = "bates", prefix = "DOC-")
val pdf = build.write(numbered)
val checks = build.validate(pdf, "3b", "2b", "3u")
val recipe = build.recipe("Review packet", pdf, checks)
val checked = PdfPipeline.check(recipe)
```

`check` accumulates independent typed `Issue`s in deterministic order. Once fields
are valid, it checks references, port kinds and cycles, then creates a stable
execution order. A PDF operation consumes a document; writing produces bytes;
validation consumes those exact bytes and produces a receipt. Arbitrary callbacks
and implementation class names are not part of a saved recipe.

After checking succeeds, `PdfPipeline.run(plan, documentsBySlot, services)` returns a Kyo computation with
`Abort[PdfError] & Sync`. Its `Services` interpreter supplies artwork and validation;
selection, concatenation and writing use Kyo PDF directly. Each reachable node runs
once. Multiple consumers share an immutable value, and intermediates are released
after their last use. Graph branches execute sequentially.

A recipe can reuse another saved recipe with
`base.include("review", fragment, Map("source" -> "body"))`. This binds fragment
input slots to existing document nodes, namespaces its other IDs and adds its
outputs. The resulting graph is checked before it is returned.

Defaults cap recipes at 24 nodes, four named document slots, three writing nodes
and eight outputs. Runtime limits cap each document at 64 pages and 8,000 objects,
each written PDF at 12 MiB, and live values at 64 MiB of counted payload and 32,000
objects. Counted payload is not total JVM heap usage. Document rewrites materialize
bounded object graphs; composing a graph does not turn every operation into an
unbounded byte-stream transformation.

[Paper Machine](../examples/paper-machine/README.md) supplies a strict JSON codec,
a visual graph editor, local file bindings, searchable artwork and isolated PDF/A
validation. Its `PipelineWiring` adapter compiles real Volga `FreeProp`/SMC wiring,
including explicit forks and assembly joins, into this same portable recipe model.
The adapter is example code; the core pipeline module stays independent of Volga.

```sh
sbt 'kyoPdfPipeline/test'
```
