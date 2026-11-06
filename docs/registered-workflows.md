# Registered, serializable workflows

Recipe version 4 adds `Registered` nodes to the existing Kyo data graph. Earlier
recipe versions keep their interpretation. Construction remains effect-free:
`>>>`, `><`, `&&&`, named branches, reusable subarrows, and checked JSON all work
with registered operations. Scala clients must recompile after the port aliases
and new enum cases in this change.

## One operation definition

`StepRegistry.Step[A, B, P]` specifies a stable ID and version, typed input/output
ports, a parameter schema, examples, capabilities, access requirements, parameter
checks, and an executor. ZIO Blocks schemas drive strict JSON decoding and JSON
Schema export. `StepRegistry.standard.exported` exports the catalog;
`PdfPipelineModel.toolSchema(withFiles = true)` exports the model-facing recipe
schema. `recipeSchemaFor(customRegistry)` and `decode(json, customRegistry)`
support host-provided operations. Registry entries are host code: a saved recipe
cannot load classes, executables, URLs, or resolvers. Duplicate registrations and
unknown versions fail checking. Parameter errors retain paths such as
`nodes[2].call.settings.dpi`.

`PdfArrow.registered(step, settings)` creates an ordinary typed arrow. Both
`PdfArrow.recipe(name, arrow, registry)` and `PdfPipeline.check(recipe, registry)`
accept custom registries. `RecipeBuilder.registered` and `registered2` support
the same operations in the wire builder. `StepRegistry.literal` supplies typed
data inputs directly from recipe data. Binary payloads stay outside JSON.

The standard registry includes byte counts, PDF facts, page collections and page
filters, policy evaluation, content selection, redaction plans, redaction,
replacement, OCR, and cleanup. New data ports carry a schema ID and version and
strictly decoded bounded object data. A recipe cannot assert a different input
or output schema than the registered definition. Data is capped at 64 KiB per
value; existing graph, page, object, file, and live-retention limits still apply.

## Redaction, OCR, and final checks

```scala
import io.github.adrielc.kyopdf.pipeline.*
import PdfArrow.*

val edit = PdfSteps.redactArrow(PdfSteps.Select(
  regions = Vector(PdfSteps.Region(1, 18, 18, 220, 47))
)) >>> registered(PdfSteps.ocr, PdfSteps.Ocr("eng"))

val packet = input("body") >>> write >>> edit >>> archivePdfA3b
val recipe = PdfArrow.recipe("Public packet", packet)
```

Coordinates use points from the top-left of an unrotated page. A selection and
redaction plan contain the SHA-256 of the exact source PDF. Out-of-range boxes,
rotated region coordinates, stale plans, empty plans, and unlocated search terms
fail rather than silently doing nothing. Automatic selection does not infer that
a scanned image contains no sensitive text: OCR or explicit regions are needed.

`pdf.redact` is a registered graph operation. Its optional local renderer
overwrites selected pixels and creates a fresh raster document. It copies none
of the source PDF's object graph: original text, image payloads, nested Forms,
unused objects, metadata, embedded files, annotations, widgets, and incremental
revisions do not survive. Final page pixels inside each selected region are
checked after saving and reopening. The receipt binds the operation and both
hashes to the exact returned bytes. Required errors never return a provisional
file, even when another requested output was already computed.

This is deliberately destructive raster editing. It loses original vectors,
searchable text, accessibility tags, links, bookmarks, interactive forms, and
signatures. Output precision depends on the chosen DPI. OCR can restore a new
search layer from the remaining pixels; it cannot restore original structure.
Redaction only removes content in the selected regions. It does not discover
every sensitive occurrence or make a legal filing decision.

`pdf.replace-text` finds text with the renderer, measures a selected base-14 font,
checks width and height, removes the old glyphs, inserts the replacement, and
raster-rebuilds the output. Current replacement fonts are Helvetica, Courier, and
Times; characters needing a Unicode font and rotated pages fail explicitly. This
is bounded region replacement, not paragraph reflow or arbitrary font editing.
`pdf.cleanup` raster-rebuilds page contents while dropping annotation and widget
appearances. `pdf.ocr` renders bounded pages and generates
a fresh searchable PDF through Tesseract. Neither cleanup nor OCR establishes
PDF/A conformance: compose archival conversion and exact-byte validation after
all edits, as above. That needs a conversion/validation service in addition to
the semantic worker.

Configure a `ProcessSemanticBackend` in `PdfPipeline.Services.semantic` on JVM
or Native. It requires an operator-chosen Python executable, the public worker
script, and a separately installed renderer. Paper Machine reads
`KYO_PDF_SEMANTIC_PYTHON` and optional `KYO_PDF_SEMANTIC_WORKER`. PyMuPDF 1.26.7
is AGPL/commercial licensed, optional, and not linked or published with the core
Scala artifacts; choose a deployment/license arrangement appropriate to your
application or implement the same service contract with another backend.

The worker accepts at most 12 MiB and 64 pages, 16 million pixels per page and
128 million pixels per run, 72–300 DPI, and 256 regions. Requests/reports are
bounded to 64 KiB. The adapter enforces an execution timeout and removes its
private workspace on success and failure. These are application budgets, not an
OS renderer sandbox; sandbox the worker for hostile inputs. No subprocess path
or network endpoint is supplied by a recipe.

## Filters, policy branches, and failures

`DataPrograms` supplies serializable comparison/all/any/not predicates for
schema data, generic bounded collection filters, and required gates. Missing or
incompatible fields produce `Unknown`. Filters can fail, keep-and-flag, or
drop-and-flag unknown items. Gates require `Yes`. There is no arbitrary scripting
language inside recipes. Recursive predicates have depth, node, path, and literal
bounds; collection results and flags have item and encoded-data bounds.

`policy.evaluate` consumes exact-source facts and emits `PolicyResult` with
`Yes`, `No`, or `Unknown` plus evidence. `PdfArrow.policyPassed` and
`RecipeBuilder.choosePolicy` use that value directly. The built-in structural
inspector supplies page count and file bytes. It leaves unavailable text metrics
unknown; it does not invent visual evidence. Hosts can register richer fact
producers under different versions.

`OnError.Fail` is the default. `RecoverAndFlag` is allowed only for a non-critical
unary step whose input and output types agree, and only for an explicitly
recoverable `Unavailable` or `Backend` error. The unchanged input is returned
with an auditable run flag. Rewrite and verification steps cannot recover this
way. Invalid input, limits, rejected checks, and failed verification stay fatal.
This prevents a failed redaction from quietly returning the unredacted original.

## Planning and storage

The planner groups total, pure byte probes sharing the same file input. They
compile into one broadcast `Scan`, share each input byte, and run in one source
pass. Probe results are retained under the live-data budget. This does not fuse
renderers or arbitrary effects, and a byte-probe pass count is not an end-to-end
PDF parse count. Pure probes in the same group can compute facts for a route
that is later skipped; no effects or required checks are speculatively run.

`ExecutionPlanner.withStaged` is the generic ingestion boundary: it copies
bounded chunks into at most 1 MiB of memory and spills beyond the configured
threshold to a scoped disk file, enforcing at most 1 GiB of spool quota and 65536 source chunks. The
source is accessible only through scoped `withInput`; streams and temporary
files close on failure too. `mapBounded` visits collections sequentially, stops
pulling at the first hard error, and can retain explicitly recoverable item
errors. It caps retained results at 1 MiB and visits at most 1024 items.

Existing `PdfPipeline.run` still accepts materialized, bounded `PdfDocument`
inputs and `PdfFile` values; its parser/writer has not become an unlimited disk
document engine. `withStaged` is available to source/backend adapters, while the
local renderer uses a disk workspace. No general constant-memory or single-pass
rewrite claim is made. Runners return outputs only after every demanded output
and required gate succeeds; external side effects inside custom executors must
provide their own transaction semantics and enforce their working-memory budgets.

## Repeatable tests

Install `scripts/semantic-requirements.txt` in a separate Python environment and
install Tesseract's English language data, qpdf, and Poppler. Then run:

```bash
export KYO_PDF_SEMANTIC_PYTHON=/path/to/venv/bin/python
bash scripts/verify-semantic-tools.sh jvm
bash scripts/verify-semantic-tools.sh native
```

Fixtures are generated synthetic PDFs containing text, an image, nested Forms, a widget,
an annotation, an attachment, metadata, and an unused secret object. Tests check
final redacted pixels, stale plans, font fitting, OCR, cleanup, graph/schema JSON,
policy branches, fail-fast/recovery behavior, shared scans, quotas, and scoped
staging. qpdf, Poppler, and pypdf independently inspect final outputs. Renderer
integration tests are opt-in locally and required by the dedicated CI job.
