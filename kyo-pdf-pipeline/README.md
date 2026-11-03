# Data-defined PDF pipelines

`kyo-pdf-pipeline` represents a PDF job as immutable, versioned data: named operations,
settings, input references and output references. The module depends on Kyo PDF;
it does not depend on ZIO, PDFBox, veraPDF, a JSON library, or Volga.

```scala
import io.github.adrielc.kyopdf.pipeline.{PdfArrow, PdfPipeline}
import PdfArrow.*

val finish: PdfArrow[Document, File] =
  select(4, 1, 3) >>> stamp("DRAFT") >>>
    number(start = 7, style = "bates", prefix = "DOC-") >>> write
val deliver = finish >>> (id[File] &&& validate("3b", "2b", "3u"))
val recipe = PdfArrow.recipe("Review packet", input("body") >>> deliver)
val checked = recipe.flatMap(PdfPipeline.check)
```

`PdfArrow[A, B]` is an immutable, reusable arrow. `>>>` and `<<<` compose
compatible types; `><` (also `***` or `*#*`) takes the tensor product of independent
arrows. `first` and `second` lift an arrow into a pair. `fork` explicitly duplicates
a wire, and `&&&` derives fanout. Braiding, associators and unit morphisms preserve
product structure. These structural operations do not write PDFs.

Construction needs no node IDs. Reusing an arrow instantiates it at each use with
fresh deterministic IDs; `named` supplies optional graph labels. To share one
execution, fork its output. Compilation preserves duplicate output positions through
aliases rather than silently deleting one. `RecipeBuilder` remains available for
existing code that constructs graph nodes directly.

The public example's `PipelineWiring` supplies an actual
`volga.SymmetricCat[PdfArrow, Objects]` and the actual `volga.syntax.smc` macro over
Document, File and Receipt types and Scala tuple products. Its `of0`–`of4` examples
cover typed application, joins, heterogeneous outputs and tuple destructuring.
Wrong PDF types, implicit copying and unused wires fail during SMC construction;
use an explicit fork or projection. The core module remains independent of Volga.

```scala
// Inside the public example, with its Volga adapter in scope:
import io.github.adrielc.kyopdf.demo.PipelineWiring.{given, *}
import volga.syntax.smc.V

val cut = select(4, 1, 3)
val join = assemble()
val split = fork[File]
val checks = validate("3b", "2b")
val packet = syntax.of2((front: V[Document], body: V[Document]) => {
  val selected = cut(body)
  val joined = join(front, selected)
  val pdf = write(joined)
  val (download, toCheck) = split(pdf)
  val receipt = checks(toCheck)
  (download, receipt)
})
```

`check` accumulates independent typed `Issue`s in deterministic order. Once fields
are valid, it checks references, port kinds and cycles, then creates a stable
execution order. A PDF operation consumes a document; writing produces bytes;
validation consumes those exact bytes and produces a receipt. Arbitrary callbacks
and implementation class names are not part of a saved recipe.

After checking succeeds, `PdfPipeline.run(plan, documentsBySlot, services)` returns a Kyo computation with
`Abort[PdfError] & Sync`. Its `Services` interpreter supplies artwork and validation;
selection, concatenation and writing use Kyo PDF directly. Each demanded node runs
once. Multiple consumers share an immutable value, and intermediates are released
after their last use. Graph branches execute sequentially.

## Conditional paths

Version 2 adds `Choose` and `Stop`; version 1 recipes remain readable. A choice
has four inputs: the condition source, the yes branch, the no branch and the
unknown branch. All three branches must return the same port kind. The runner
evaluates the condition first, then demands only the selected branch. Choices
can nest and rejoin shared steps. An unselected branch does not load its inputs,
rewrite PDFs, validate files or trigger its stop operation.

Predicates cover page-count ranges, declared PDF/A-3b metadata and completed
validation verdicts. A metadata claim is not conformance. Missing validation
results, conflicting metadata or unreadable metadata take the unknown path; a validator's inability to check is
distinct from a failed conformance check. Custom services provide typed verdicts
through `validationReport`; the older string-only `validate` method remains
supported and safely yields unknown when used as a condition.

```scala
val route: PdfArrow[Document, Document] = choose(pages(1, 3))(
  stamp("SHORT PACKET"),
  stamp("LONG PACKET"),
  stop[Document, Document]("Page count is unavailable")
)
val conditional = PdfArrow.recipe("Route a packet", input("body") >>> route >>> write)
```

Branches are reusable `A → B` arrows with a common domain and codomain, followed
by ordinary composition. `pages`, `declares3b` and `verified(profile)` construct
closed predicates; `condition.on(arrow)` composes a typed condition source. The
chosen branch can return a document, written file or receipt, then fork or chain
into further operations. No arbitrary Scala callback is saved in a recipe.

Version 3 adds explicit `Keep` projections and `StopAfter`. `keepLeft` and
`keepRight` evaluate all connected inputs before returning the selected value;
`stop[A, B]` evaluates its input before stopping. Thus `validate(...) >>> stop(...)`
actually performs the check. This sequencing also survives JSON roundtrips. A
stop inside an unselected conditional branch remains skipped. Version 1 and 2
recipes remain readable, and including a newer fragment promotes the result.
Every branch is checked for valid fields, references, port kinds and cycles before
execution. `Run` records executed IDs, skipped IDs and each decision's outcome,
selected branch and evidence. Tensor execution remains sequential and bounded.

A recipe can reuse another saved recipe with
`base.include("review", fragment, Map("source" -> "body"))`. This binds fragment
input slots to existing document nodes, namespaces its other IDs and adds its
outputs. The resulting graph is checked before it is returned, and including a
newer fragment promotes the resulting recipe to that version.

Defaults cap recipes at 24 nodes, four named document slots, three writing nodes
and eight outputs. Runtime limits cap each document at 64 pages and 8,000 objects,
each written PDF at 12 MiB, and live values at 64 MiB of counted payload and 32,000
objects. Counted payload is not total JVM heap usage. Document rewrites materialize
bounded object graphs; composing a graph does not turn every operation into an
unbounded byte-stream transformation.

[Paper Machine](../examples/paper-machine/README.md) supplies a strict JSON codec,
a visual graph editor, local file bindings, searchable artwork and isolated PDF/A
validation. Its `PipelineWiring` adapter uses real Volga SMC wiring over the typed `PdfArrow` category, including
explicit forks, assembly joins and reusable conditional arrows, to compile this
same portable recipe model.
The adapter is example code; the core pipeline module stays independent of Volga.

```sh
sbt 'kyoPdfPipeline/test'
```
