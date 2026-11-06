# Registered, serializable workflows

Recipe version 4 adds `Registered` nodes to the existing Kyo data graph. Earlier
recipe versions keep their interpretation. Construction remains effect-free:
`>>>`, `><`, `&&&`, named branches, reusable subarrows, and checked JSON all work
with registered operations. Scala clients must recompile after the port aliases
and new enum cases in this change.

## Schema-driven model authoring

`PdfPipelineModel.authoring(Set("input", "write", "pdf.select-content",
"pdf.plan-redaction", "pdf.redact", "pdf.ocr"))` returns a scoped definition:
the JSON Schema, instructions generated from operation signatures and descriptions,
and strict `decode` / `decodeToon` functions. The model response contract is
always JSON (`responseFormat: "json"`), including when its context is TOON. Unavailable operations are excluded
from the exported schema and rejected by the decoder. Registered operations use
their exact input arity. Parameters are decoded and checked against the registry's
`DynamicSchema` constraints before graph checks; a failed decode never falls back
to an unvalidated settings string.

Recipe records have one ZIO Blocks `Schema`. JSON and TOON codecs are derived
automatically from it with strict field checking; TOON has no separate recipe
model or operation-specific serializer. `PdfPipelineToon.encode(checked)` and
`decode(text, registry)` round-trip the same checked graph, including named branches
and policy parameters. Both transports run on JVM and Native. The public-source
TOON support module includes the compatibility fixes described in
[`modules/zio-blocks-native/README.md`](../modules/zio-blocks-native/README.md).

TOON admission checks bound text to 32 KiB, 2048 lines, 64 indentation levels,
2048 elements per declared array and 8192 declared elements in total before
parsing. Numbers requiring more precision than the pinned TOON reader preserves
are rejected; use JSON for those values. Final graph, parameter and operation
scope checks apply after either decoder. `DynamicSchema` is an in-memory contract,
not an executable schema reconstructed from a text round trip.

Paper Machine exposes the definition at
`GET /api/pipelines/authoring?operations=input,write,pdf.ocr&format=json`.
Use `format=toon` for its TOON context representation. POST the model's JSON
candidate as the raw body to `/api/pipelines/authoring/check` with the same
operations and `format=json`, regardless of the context encoding. A successful check
returns normalized compact JSON and execution order; errors retain repairable
paths such as `nodes[3].yes` or `nodes[2].settings.dpi`. Pass the returned recipe to
`run_pdf_pipeline` with uploaded document bindings.

The optional local-model evaluation obtains each scoped definition from this
API in JSON or TOON as context, invokes OpenCode with a local Ollama model,
requires a JSON response, checks it, and permits
at most two repairs using checker feedback. It executes valid responses through
the actual MCP tool and independently checks synthetic PDF outputs:

```sh
# Start Paper Machine with the semantic worker, as described below, then:
python scripts/test-local-model-pipelines.py http://127.0.0.1:8087 \
  --model gemma4:26b --context-formats json toon --output /tmp/pdf-authoring-evidence
```

Use the semantic-worker Python environment for this command. It requires
OpenCode, Ollama, qpdf, Poppler and an installed local model. Cases cover redaction
and OCR, replacement, fused byte counts, both conditional branches, body-only
numbering with bookends, and an OCR failure that must expose no output. It supplies
no completed target recipe and records candidates, repairs, timings, tool calls,
hashes and returned PDFs. This evaluation is optional and does not download models
or run inference in CI.

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
`nodes[2].settings.dpi` in the model-facing format.

Model-facing nodes need only an operation, settings, and wiring. Port schemas
come from the host registry, not from model-authored type claims:

```json
{
  "version": 4,
  "name": "Redact and OCR",
  "nodes": [
    {"id": "body", "op": "input", "slot": "body", "inputs": []},
    {"id": "pdf", "op": "write", "inputs": ["body"]},
    {"id": "select", "op": "pdf.select-content", "inputs": ["pdf"], "settings": {
      "regions": [{"page": 1, "x": 18, "y": 18, "width": 220, "height": 47}]
    }},
    {"id": "plan", "op": "pdf.plan-redaction", "inputs": ["select"]},
    {"id": "redact", "op": "pdf.redact", "inputs": ["pdf", "plan"]},
    {"id": "ocr", "op": "pdf.ocr", "inputs": ["redact"]}
  ],
  "outputs": ["ocr"]
}
```

Settings may be omitted when defaults pass the registered parameter checks.
The exported tool schema uses the same operation definitions and marks required
settings accordingly. Wiring is checked against the inferred ports before a PDF
is accessed. Wrong connections retain paths such as `nodes[4].inputs[1]`.

An operation version can be omitted only when its ID has one registered version.
If several versions are available, specify `"version": 1` on that node; the
decoder never silently chooses the newest version. `PdfPipelineModel.encode`
uses the checked plan's registry and preserves explicit versions when ambiguous.
`PdfPipelineJson.encode(checked)` is the canonical persistence format: it stores
resolved operation versions and full typed calls, so adding a registry version
does not change the meaning of that saved graph. Loading rechecks the registry
and all connections. Older model recipes using `"op": "registered"` and a
verbose `call` remain readable; their declared ports must match the registry.

For `run_pdf_pipeline`, put this recipe under `arguments.recipe` and bind
`arguments.files.body` to an uploaded document ID. `check_pdf_pipeline` accepts
the same compact recipe without files. Named `yes`, `no`, and `unknown` branches
and ordinary arrow composition retain their existing behavior.

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

The Native pipeline artifact declares the public `scala-native-crypto` dependency
for SHA-256. Native consumers need OpenSSL available to the linker (`libssl-dev`
on Debian/Ubuntu or Homebrew OpenSSL on macOS); the standalone consumer proof
executes a known digest without a test framework on its classpath.

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
