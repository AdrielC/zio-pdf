<!-- Product context: scoped to the Paper Machine example, not the library. -->
# Paper Machine

Paper Machine is a local visual studio for exploring, remixing, and validating PDFs.

## People and purpose

Developers and curious PDF readers can see the object graph behind a rendered page,
follow references, select pages in a new order, and download a rewritten document.
They can also prepend/append PDFs, add searchable text stamps and page or Bates
numbers, and validate the finished output in the same build workflow.
The first visit opens a synthetic four-page specimen so the studio works immediately.

The brief delegates the visual direction: “Surprise me with the visual studio.”
The confirmed workflow is code-first: build the working interface directly.

## Product facts

- Platform: desktop web with a responsive layout for smaller screens.
- Stack: Scala, Kyo PDF, the JDK HTTP server, and dependency-free HTML/CSS/JavaScript.
- Kyo PDF parses, selects, and writes documents. PDFBox renders PNG previews.
- The server binds to loopback. Files stay on the local machine; exploration is
  held in memory and validation uses temporary files removed after each check.
- Explorer input is limited to 12 MiB and 1–64 pages. Three documents and twelve previews
  are retained; older entries expire. The graph displays at most 180 objects.
- Selection supports distinct pages in an explicit order. Repeated pages are rejected.
- Packet assembly combines a front PDF, selected body pages and a back PDF. Stamps
  and footer numbers follow final order. Kyo keeps source page objects and writes
  the output; PDFBox authors embedded-font, ICC-colored artwork only. Two bounded
  bookend slots supplement explorer retention. Final output remains capped at 64
  pages and 12 MiB. Interactive form merges are rejected; only the body's catalog
  is retained. Finishing options reset after each build to prevent accidental reuse.
- Validation queues up to twenty files, each up to 12 MiB. Explicit PDF/A-3b,
  PDF/A-2b, and PDF/A-3u profiles share a veraPDF parse per file. Files run sequentially.
- Identification reads bounded, namespace-aware XMP from the current catalog.
  A metadata declaration is distinct from a conformance verdict. Only completed
  veraPDF passes are verified; unsupported, interrupted, and unreadable files stay unknown.
- Validation uses disposable local files and a 384 MiB worker JVM with a 45-second
  deadline. Receipts record the exact bytes' hash, profile, engine version and time.
  Rewritten files need a fresh check. JSON/CSV batch reports are downloadable.
- The studio is an example, not an editor for text, forms, signatures, or encryption.
  Rewriting does not preserve digital signature validity.

## Success

A first-time visitor can inspect a page's underlying dictionary, reorder the sample,
and obtain a real PDF whose page order and trailer match the rewritten document.
Controls, errors, loading states, keyboard focus, and responsive behavior work.
Several files and profiles can be checked with one action. Each failure is local
to its file, with useful rule evidence and an option to inspect the document.

## Content and provenance

The built-in specimen is synthetic vector artwork authored in this public example.
All implementation and regression inputs come from the public repository or are
newly authored. There are no external document uploads or commercial claims.

## Assumptions

This first surface favors discovery over dense professional editing. Native PDF
text editing and annotation are outside this example's scope.
