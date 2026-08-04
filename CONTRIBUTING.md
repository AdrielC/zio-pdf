# Contributing to zio-pdf

Canonical forge: [git.internal.net/internal/zio-pdf](https://git.internal.net/internal/zio-pdf)

## Workflow

1. Clone and branch from `main`:
   ```bash
   git clone https://git.internal.net/internal/zio-pdf.git
   cd zio-pdf
   git checkout -b feat/my-change
   ```
2. Make changes; keep public API on **`PdfEngine`** / **`PdfStream`** / **`PdfIO`**.
3. Run locally:
   ```bash
   sbt test
   sbt examples/run
   sbt "bench/Jmh/compile" "benchFs2/Jmh/compile"
   ```
4. Open a pull request on internal → squash merge into `main`.
5. `main` is protected: **CI must pass** and **one approval** is required (no self-merge).

## Code conventions

- Scala 3, ZIO 2 — prefer `import zio.*` star imports in `zio.pdf` code.
- Avoid `throw` in library code; use `ZIO` / `Validation` / `Attempt`.
- Public byte surface: **`Chunk[Byte]`**, not `Array[Byte]`.
- Fused mmap paths stay behind **`PdfEngine`**; do not expose `PdfHyperdrive` publicly.
- Match existing naming: `decoded()` pipeline vs `decode(path)` fused collect.

## Commit messages

Short imperative subject, optional body explaining *why*:

```
feat: add PdfEngine.elementsSink for constant-memory classify
fix: digestSink Chunk typing on ZIO 2.1.25
docs: update pipeline-first README examples
```

## Releases

Maintainers tag on `main` (`v0.2.0-RC1`, …) and publish Gitea releases from `CHANGELOG.md`.
