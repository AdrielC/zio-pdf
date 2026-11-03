# Paper Machine

A local PDF object explorer and page remix studio powered by Kyo PDF.

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

## What runs where

- Kyo PDF decodes the object graph, resolves the page tree, selects pages in the
  requested order, remaps references, and writes the PDF with a matching trailer.
- PDFBox renders PNG previews. It does not select or rewrite the PDF.
- A dependency-free browser interface talks to the local JDK HTTP server.
- No PDF leaves the process for a remote service. Documents are held in memory;
  the latest three documents and twelve previews are retained until exit or eviction.
- Input is capped at 12 MiB, 8,000 objects, 150,000 retained nodes, depth 32,
  32 MiB retained payload, and 64 pages. The displayed graph is capped at 180
  objects, always includes page dictionaries, and reports truncation. Dictionary
  previews abbreviate deep nesting and long values. Preview rendering is best effort; these limits
  are not a sandbox for hostile rendering workloads.
- Pages may be selected once each. This example does not edit page text or forms,
  support encrypted PDFs, or preserve digital signature validity after rewriting.

The browser has no build step or remote asset dependencies. Chivo is self-hosted
from the public [Google Fonts source](https://github.com/google/fonts/tree/main/ofl/chivo)
under the [SIL Open Font License](FONT-LICENSE.txt). All sample artwork is authored
in `Sample.scala`, using the Kyo PDF document algebra.

## Checks

```sh
sbt ';kyoPdf/testOnly *PdfParserSpec;paperMachine/compile'
python3 examples/paper-machine/test_studio.py http://127.0.0.1:PORT
```

The live integration checks need a running studio and use synthetic documents only.
Install `qpdf` and Poppler (`pdftotext`) to include independent PDF validation and
page-order verification; checks that need missing executables are explicitly skipped.
The core regression suite covers ordered selection, subsets, invalid selections,
inherited page attributes, retained metadata, and rewritten trailer normalization.
The synthetic source-object collision regression also opens the exported PDF with
Apple PDFKit when running on macOS with Swift installed. This catches invalid page
parents that more permissive readers may accept.
