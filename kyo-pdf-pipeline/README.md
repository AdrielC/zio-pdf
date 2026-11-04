# Data-defined PDF pipelines

`kyo-pdf-pipeline` represents a PDF job as immutable, versioned data: named operations,
settings, input references and output references. The module uses Kyo PDF and ZIO
Blocks Schema for JSON transport and page-count expressions.

```scala
import io.github.adrielc.kyopdf.pipeline.{PdfArrow, PdfPipelineJson}
import PdfArrow.*

val finish: PdfArrow[Document, File] =
  select(4, 1, 3) >>> stamp("DRAFT") >>>
    number(start = 7, style = "bates", prefix = "DOC-") >>> write
val deliver = finish >>> (id[File] &&& validate("3b", "2b", "3u"))
val json = PdfPipelineJson.encode("Review packet", input("body") >>> deliver)
val checked = json.flatMap(PdfPipelineJson.decode)
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
execution order. `PdfPipelineJson.decode` performs the same checks after parsing.
The JSON contains the executable recipe, including input slot names. Supply PDFs
and a services interpreter separately when running it. The studio has its own
editor JSON format. A PDF operation consumes a document; writing produces bytes;
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
validation verdicts. The interpreter reconstructs a ZIO Blocks `SchemaExpr` from
the saved page-count bounds and evaluates it against the document page count.
The declaration predicate reads the PDF's metadata claim. Missing validation
results, conflicting metadata and unreadable metadata take the unknown path.
A failed conformance check takes the no path. Custom services provide typed verdicts
through `validationReport`; the older string-only `validate` method remains
supported and safely yields unknown when used as a condition.

```scala
val route: PdfArrow[Document, Document] = choose(pages(1, 3))(
  stamp("SHORT PACKET"),
  stamp("LONG PACKET"),
  stop[Document, Document]("Page count is unavailable")
)
val conditional = PdfPipelineJson.encode("Route a packet", input("body") >>> route >>> write)
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
sbt 'kyoPdfPipeline/testFull'
```

The same Policy and pipeline sources and tests run on Scala Native 0.5.12.
Use the published Native module, which brings its public-source Schema runtime
and Kyo core as transitive dependencies:

```scala
libraryDependencies += "io.github.adrielc" % "kyo-pdf-pipeline_native0.5_3" % "0.6.0"
```

```sh
sbt 'kyoPdfPipelineNative/testFull'
bash scripts/verify-native-policy-consumer.sh
```

The Native runtime is built from [pinned public ZIO Blocks sources](../modules/zio-blocks-native/README.md).
There are no supplied-JAR settings or repository overrides. Consumers do not need
`-experimental`. Compile-time nominal schema derivation, SchemaExpr conditions,
JSON Schema export, recipe loading, and Policy execution work on Native.
JVM reflection is unavailable. Artwork, PDF/A conversion and archival validation
still require an appropriate `Services` interpreter; the Paper Machine server
and its PDFBox/veraPDF implementation run on JVM.

## Author recipes with a model

Use `PdfPipelineModel.recipeSchema.print` to export the named-branch JSON Schema,
or `PdfPipelineModel.toolSchema(withFiles = true).print` to include file bindings
in a tool's argument schema. `PdfPipelineModel.decode` checks authored JSON and
returns a runnable plan. `PdfPipelineModel.encode` gives a model-facing example
from any checked recipe, including recipes compiled from typed arrows. Field
examples come from the same operation and predicate registry as the decoder.

`PdfPipeline.explain` resolves conditions backed by bound inputs. It reports
conditions that depend on downstream processing as deferred. Use the same
`choose`, `>>>`, `><`, and `&&&` combinators to chain, split, and rejoin the
arrows before exporting a recipe. A `hasPageNumbers` condition uses the
interpreter's page-number evidence and returns unknown by default.

`PdfArrow.convertPdfA3b` is a `File → File` arrow backed by the interpreter's
conversion service. Use `PdfArrow.archivePdfA3b` for a `File → (File, Receipt)`
arrow that converts, validates the converted bytes, and returns both only on a
passing PDF/A-3b verdict. Compose it after the final write so all page edits are
included in the validation.

## Policies as data

A `Policy.Profile` is a versioned validation program with an authority, document
type, effective date, source citations, and rules. Serialize it with
`Policy.encode` and load it with `Policy.decode`. Loading checks the
profile and compiles its comparisons into ZIO Blocks `SchemaExpr` expressions.
`Policy.Checked.run` evaluates the plan against observations supplied by
an inspector and accumulates findings.

### Runtime results (v0.6.0)

Runtime output payloads now carry checked domain types:

```scala
enum Value:
  case Document(value: PdfDocument)
  case File(value: PdfFile)
  case Receipt(value: PdfReceipt)
```

`PdfFile.checked` accepts an immutable encoded payload with a PDF header and a
fixed 64 MiB supported maximum. It retains the vector without copying it. This
does not establish PDF syntax or archival conformance. Runtime file limits may
be smaller, and the writer checks encoded size before allocating its output,
including decimal expansion and sparse cross-reference tables.

`PdfReceipt.Validation` holds a `VerificationResult` whose verdict keys are
`PdfAProfile` values. `PdfReceipt.Inspection` holds application-specific report
data and cannot supply archival verdicts. Both retain the exact `PdfFile`
provided to the interpreter. The live-value budget counts that subject even
when only a receipt is returned, and deduplicates shared subjects.

`ReportJson.parse` produces an object-shaped report with limits of 1,048,576
UTF-16 code units, 64 levels and 32,768 JSON nodes. It rejects duplicate fields.
The typed `Services.verify`, `convert` and `inspect` entry points use these
payloads. Existing string/byte callbacks remain compatibility adapters; malformed
reports, unknown profiles and unrequested verdicts fail at that boundary.

This changes the Scala runtime-result API. Read `file.bytes` and
`receipt.report.value` (or `.encoded`) explicitly. Typed service implementations
can override `verify(file, profiles)` and return `VerificationResult` directly.
Saved recipe JSON, model-facing schemas and the studio result envelope retain
their existing format. Recompile callers migrating from v0.5.0 and follow the
[v0.6.0 migration notes](../release-notes/v0.6.0.md).

Rules select document, page, or bookmark scope. Conditions support comparisons
between facts and numbers, `All`, `Any`, `Not`, and `Review`. The optional
`when` condition limits applicability. A false condition selects the rule's
failure status. Missing evidence, manual review conditions, and unresolved
applicability produce `needs-review`. Each finding retains its rule, source,
page, and measured evidence.

```scala
import io.github.adrielc.kyopdf.pipeline.{Policy, PdfArrow}
import Policy.*

val size = Condition.Compare(
  Operand.Fact(Metric.FileBytes),
  Comparison.LessThanOrEqual,
  Operand.Number(20_000_000)
)
val rules = Profile(
  version = 1,
  id = "example-report-v1",
  name = "Example report checks",
  authority = "Example organization",
  documentType = "Report",
  effectiveDate = "2026-11-03",
  source = "Example document standards",
  rules = Vector(Rule(
    "file-size", Scope.Document, size,
    "Example rule 1", "Example document standards",
    "The PDF must fit within 20 MB."
  ))
)
val saved = Policy.encode(rules)
val loaded = Policy.decode(saved)
val workflow = PdfArrow.input("body") >>> PdfArrow.write >>>
  (PdfArrow.id[PdfArrow.File] &&& PdfArrow.preflight(rules))
```

Export `Policy.jsonSchema.print` for model authoring. The pipeline tool
schema embeds the same definition for the `preflight-rules` operation.
The California Second District petition preset is expressed through this API.

The available facts cover bytes, page and bookmark counts, extracted text,
pagination evidence, and bookmark destinations. Express other requirements
as `Review` until an inspector can supply reliable evidence for them.
