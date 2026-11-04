# Paper Machine

A local PDF explorer, page remix studio, and batch PDF/A validator powered by Kyo PDF and veraPDF.

```sh
sbt 'paperMachine/run'
```

Open the `PAPER_MACHINE_URL` printed by the server. The default is a free port on
`127.0.0.1`. To choose a port: `sbt 'paperMachine/run 8087'`. Stop with Ctrl-C.
Use the printed loopback address; other hosts and cross-origin requests are rejected.

The studio starts with four synthetic vector-art pages. Select an object to trace
its references and read its PDF dictionary. Choose a page to preview it, move or
remove pages in the assembly row, then **Build PDF**. The object map updates to the
rewritten document, and **Download PDF** saves the result. Removed pages can be added
back from the page bench before building. You can also open or drop your own PDF.

## Validate a batch

Open **Validate PDFs**, add up to twenty files, choose profiles, and press
**Validate queue** once. PDF/A-3b is selected by default. PDF/A-2b and PDF/A-3u
can be selected in the same pass: these profiles share the PDF 1.7 model, and one
veraPDF parser serves all selected profiles for each file. Files run sequentially,
with per-file progress, independent verdicts, and a **Stop after this file** action.
A failed or unreadable file does not stop later files.

**Try three synthetic examples** loads an original, valid PDF/A-3b with a text
attachment, a false claim with a prohibited action, and ordinary vector field notes.
Use **Evidence & failure details** for rule clauses, context, counts, timestamps,
and SHA-256 fingerprints. Download the batch as JSON or a spreadsheet-safe CSV.
**Inspect PDF** opens a queued file in the existing object explorer.

XMP identification is a producer claim. Kyo follows the active catalog metadata,
supports namespace aliases and compressed/UTF-16 XMP, rejects unsafe XML, and reports
conflicting or unreadable claims. Only a completed veraPDF pass produces **Verified**.
Changing profiles clears old verdicts. Rewriting a document changes its fingerprint;
its earlier validation does not transfer. **Queue this PDF for validation** checks
the actual rewritten bytes in a new pass.

## Build a finished packet

In **Explore & remix**, arrange the source pages, then use **Finish the packet**:

- Choose a front PDF and/or back PDF; all their pages are included.
- Add a searchable text stamp at the top right or center.
- Add footer page numbers, plain numbers, or six-digit Bates numbers with an
  optional prefix and starting number. Numbering follows the complete final order.
- Optionally validate the exact finished PDF in the same run, using the profiles
  selected in **Validate PDFs**. Other queued receipts remain available.

One build performs selection, concatenation, artwork overlay and a final Kyo write.
PDFBox authors only the embedded-font, ICC-colored overlay artwork; Kyo retains
source page objects and writes the complete document. Rotation, crop offsets and
resource-name collisions are handled without rasterizing source pages. A synthetic
PDF/A-3b with an attachment passes veraPDF after stamping and numbering, but every
output still requires its own verdict. Changing arbitrary PDFs cannot guarantee
PDF/A conformance.

The packet is limited to 64 pages, 8,000 source objects and 12 MiB of final PDF bytes.
Two bounded bookend slots are retained separately from the three explorer documents.
Combining interactive forms is rejected. The primary document's catalog is retained;
other inputs' document-level outlines, names and metadata are not merged. New marks
are page content, not editable annotation stamps or redactions. Finishing options
reset after a build so another click does not accidentally stamp the result again.

## Compose saved pipelines

Open **Pipelines**. The initial recipe is authored with the vendored Volga SMC
syntax and compiled into plain version 1 data. It selects, stamps, numbers, writes,
and validates the synthetic sample. The sample is an ordinary PDF, so its PDF/A
checks correctly fail; a declaration or rewrite alone is not conformance.

Use **Try a recipe** for two conditional examples. **Route by page count** chooses
a short- or long-document stamp, then rejoins at a shared write and validation.
**Route by validation** returns an already conforming PDF unchanged, stamps a
failed PDF for review, and stops with a reason when a completed verdict is missing.
The first conditional example also compiles from real Volga SMC wiring.

- Select an operation in the graph or the operation selector, then change its
  settings or input connections. Add document operations after a selected document;
  file outputs and validation receipts can branch from shared steps.
- Bind named inputs to the current PDF or choose separate PDFs. Recipes save slot
  names and settings; they contain no document bytes, local paths, or session IDs.
  Only inputs demanded by the selected path need a binding.
- **Choose a path** supports page-count ranges, declared PDF/A-3b metadata and
  completed PDF/A verdicts. Connect yes, no and unknown branches of the same kind:
  documents, written files or receipts. Choices can nest and rejoin. Unknown
  evidence has its own route; a claim is not a completed validation pass.
- **Stop with reason** rejects a selected path with an explicit message. Skipped
  branches do not load inputs, stamp, write, validate or stop the run.
- **Capture current packet** converts the explorer's page order, front/back PDFs,
  stamp, numbering and selected validation profiles into a recipe.
- **Save recipe** and **Load recipe** round-trip JSON. **Recipe JSON** also supports
  direct data editing, with field, port, reference and cycle diagnostics.
- **Insert saved recipe** connects another recipe's named document inputs to
  existing document nodes. Its operation IDs are namespaced and its outputs added;
  shared source steps still execute once.
- **Run pipeline** writes up to three PDFs and produces receipts. Inspect or download
  each output and download the complete execution report. The graph marks skipped
  operations with dashes; the results explain each decision and list the executed
  and skipped IDs, validation passes, checked profiles and exact byte hashes.

Version 1 and 2 recipes remain readable. Version 3 preserves arrow projections and
stops that evaluate their inputs first. In static
graphs, identical bytes share a pass across the requested profiles. Conditional
graphs check only demanded profiles and reuse completed checks when they cover the
same bytes and profiles. At most three validation passes may start per run; combine
profiles in one validation operation when several checks are needed together.

The [pipeline module](../../kyo-pdf-pipeline/README.md) also offers typed Scala
authoring and a Kyo interpreter. The example's `PipelineWiring.scala` demonstrates
actual Volga SMC authoring over typed `PdfArrow[A, B]` operations, compiled to the
same data. Arrows support composition, tensor products, explicit forks, typed joins
and reusable conditional branches; mismatched types are rejected during construction. Graphs execute sequentially;
unselected and unreachable nodes are skipped and dead intermediates released. The editor supports
24 operations, four separately retained input PDFs and eight file/receipt outputs.
Each PDF is limited to 64 pages and 12 MiB. Counted live payload is capped at 64 MiB
and 32,000 objects; these are retention budgets, not total JVM heap limits.
Recipe JSON is capped at 32 KiB, depth 16 and 4,096 values. Unknown fields, duplicate
JSON keys, unsupported versions and invalid port connections are rejected.

## What runs where

- Kyo PDF decodes the object graph, resolves the page tree, selects pages in the
  requested order, remaps references, and writes the PDF with a matching trailer.
- PDFBox renders PNG previews. It does not select or rewrite the PDF.
- A dependency-free browser interface talks to the local JDK HTTP server.
- No PDF is sent to a remote service. The explorer retains three documents and
  twelve previews in memory. Validation writes a short-lived local PDF and report;
  both are removed after the isolated worker exits, including timeout/error paths.
- Validation accepts 12 MiB per file, with one upload and one worker active at a
  time. The worker has a 384 MiB Java heap and a 45-second deadline; reports are
  capped at 256 KiB. Other JVM/native memory and explorer retention are additional.
  Worker failures and timeouts produce **Unable to check**, never conformance.
- Input is capped at 12 MiB, 8,000 objects, 150,000 retained nodes, depth 32,
  32 MiB retained payload, and 64 pages. The displayed graph is capped at 180
  objects, always includes page dictionaries, and reports truncation. Dictionary
  previews abbreviate deep nesting and long values. Preview rendering is best effort; these limits
  are not a sandbox for hostile rendering workloads.
- Pages may be selected once each. This example does not edit page text or forms,
  support encrypted PDFs, or preserve digital signature validity after rewriting.

veraPDF 1.30.3 is bundled as an example-only dependency, using its public
[Java API](https://docs.verapdf.org/develop/) and explicit validation profiles.
It is available under MPL 2.0 or GPLv3+; this integration uses the MPL option.
The core Kyo library has no veraPDF dependency. Up to 24 failure examples are
returned per profile; aggregate results include all completed checks.

The browser has no build step or remote asset dependencies. Chivo is self-hosted
from the public [Google Fonts source](https://github.com/google/fonts/tree/main/ofl/chivo)
under the [SIL Open Font License](FONT-LICENSE.txt). All sample artwork is authored
in `Sample.scala`, using the Kyo PDF document algebra.

## Checks

```sh
sbt ';kyoPdf/testOnly *PdfParserSpec *PdfAIdentificationSpec *PdfAssemblySpec;kyoPdfZio/testOnly *PdfZIOSpec;paperMachine/compile'
python3 examples/paper-machine/test_studio.py http://127.0.0.1:PORT
PAPER_MACHINE_SLOW_TESTS=1 python3 examples/paper-machine/test_studio.py http://127.0.0.1:PORT ValidationTest.test_busy_timeout_and_cleanup
```

The live integration checks need a running studio and use synthetic documents only.
Install `qpdf` and Poppler (`pdftotext`) to include independent PDF validation and
page-order verification; checks that need missing executables are explicitly skipped.
The core regression suite covers ordered selection, subsets, invalid selections,
inherited page attributes, retained metadata, and rewritten trailer normalization.
The synthetic source-object collision regression also opens the exported PDF with
Apple PDFKit when running on macOS with Swift installed. This catches invalid page
parents that more permissive readers may accept.
Assembly tests cover colliding object numbers, inherited/rotated/cropped geometry,
resource and graphics-state isolation, searchable Unicode stamps, sequential Bates
numbers, front/back page order, rejected form merges, and a stamped PDF/A-3b pass.
PDF/A tests cover valid attached specimens, false claims, multiple profiles and
files, unsafe/stale/conflicting XMP, exact-byte revalidation, upload bounds, worker
backpressure, timeout, and temporary-file cleanup. The slow test suspends its own
validation worker to exercise the real 45-second deadline.

Pipeline regressions use only synthetic PDFs:

```sh
python3 examples/paper-machine/test_pipelines.py http://127.0.0.1:8087
```

## Call a pipeline as an MCP tool

Connect a local MCP client to `http://127.0.0.1:8087/mcp`, using the port
printed by Paper Machine. The endpoint uses Streamable HTTP with JSON responses.
Upload a PDF through the studio or POST its bytes to
`/api/pipeline-inputs/body`. The upload response contains the document ID.

After initialization, discover `run_pdf_pipeline` with `tools/list`.
Send a `tools/call` request like the one below, replacing the document ID
with the ID from your upload.

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "run_pdf_pipeline",
    "arguments": {
      "files": {"body": "YOUR_DOCUMENT_ID"},
      "recipe": {
        "version": 2,
        "name": "Number single-page PDFs",
        "nodes": [
          {"id": "body", "op": "input", "slot": "body", "inputs": []},
          {"id": "numbered", "op": "number", "inputs": ["body"]},
          {"id": "route", "op": "choose",
           "test": {"kind": "page-count", "min": 1, "max": 1},
           "source": "body", "yes": "numbered", "no": "body", "unknown": "body"},
          {"id": "pdf", "op": "write", "inputs": ["route"]}
        ],
        "outputs": ["pdf"]
      }
    }
  }
}
```

The result includes branch decisions in `structuredContent.decisions` and
a downloadable `application/pdf` resource link for each PDF output. Download
links use the studio's local document cache and are available while the output
remains cached. Recipe errors include paths such as `nodes[2].yes`.

Run the HTTP integration check against a running studio.

```sh
python3 examples/paper-machine/test_pipelines.py http://127.0.0.1:8087 \
  PipelineTest.test_mcp_tool_returns_pdf_and_named_branch_decision
```

`check_pdf_pipeline` takes just a recipe and returns the checked, named-branch
form and node order. `explain_pdf_pipeline` also takes file bindings and reports
resolvable decisions, deferred conditions, and expected output kinds. It
does not stamp, write, or validate PDFs. `run_pdf_pipeline` includes the
recipe version, input hashes, output hashes, executed and skipped nodes, and
validation evidence keyed by the SHA-256 of the bytes checked.

Try **Cover, conditional numbering, and tail** in the pipeline example picker,
or fetch the tool-ready recipe from `/api/pipelines/model-example?packet`.
Bind `cover`, `body`, and `tail`. The body condition looks for consecutive
page labels in extractable header and footer text. Confident absence adds
numbers to the body only. Ambiguous or unreadable pages stop for review.
The assembled PDF and the PDF/A-3b receipt come from two branches of the
same write operation. A receipt can report a failed conformance check.

### Produce a verified PDF/A-3b packet

Run Gotenberg locally and launch Paper Machine with its port configured.

```sh
docker run --rm -p 127.0.0.1:3000:3000 gotenberg/gotenberg:8
```

In another terminal, start Paper Machine.

```sh
KYO_PDF_GOTENBERG_PORT=3000 sbt 'paperMachine/run 8087'
```

Pick **Verified PDF/A-3b packet** or fetch
`/api/pipelines/model-example?archive-packet` for a tool-ready recipe.
The archival arrow sends the finished PDF to the local converter and checks
the returned bytes with veraPDF. Failed or unavailable validation stops the
pipeline. Gotenberg reprocesses the PDF during conversion.

Run the end-to-end test against the configured studio.

```sh
KYO_PDF_TEST_GOTENBERG=1 python3 examples/paper-machine/test_pipelines.py \
  http://127.0.0.1:8087 PipelineTest.test_gotenberg_archive_packet_passes_3b_through_tool
```
