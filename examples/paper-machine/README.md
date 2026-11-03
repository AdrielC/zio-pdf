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
