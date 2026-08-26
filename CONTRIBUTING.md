# Contributing to zio-pdf

This GitHub repository is the public source and release project. Every change intended for Maven Central must build without private resolvers, private artifacts, or credentials.

## Workflow

1. Clone `https://github.com/AdrielC/zio-pdf.git` and branch from `main`.
2. Keep byte streams incremental. Any collecting helper must state and enforce its bound or return a type whose full materialization is the method's explicit contract.
3. Keep public decode APIs on `PdfEngine`, `PdfStream`, and `PdfIO`. Use `PdfObjectScanner` for bounded structural observation.
4. Run:

   ```bash
   npm ci
   sbt -batch ";root/test;scanKyo/test;scalaJs/test;bench/test"
   sbt -batch examples/run
   sbt -batch "bench/Jmh/compile" "benchFs2/Jmh/compile"
   npm --prefix examples-js/frontend ci
   npm --prefix examples-js/frontend run build
   bash scripts/audit-published-artifact.sh
   bash scripts/verify-external-consumer.sh
   sbt -batch publishLocal
   ```

5. Open a GitHub pull request. CI must pass before merge.

## Code conventions

- Use Scala 3 and ZIO 2 idioms.
- Prefer typed errors and explicit error translation at transport boundaries.
- Do not use `runCollect`, `toArray`, or full-payload buffers on an arbitrary-size streaming path.
- Bounded parser carry and content-stream payload handling are separate concerns. Document both.
- Avoid `throw` in operational library paths. Constructors may reject impossible static configuration, but data failures belong in typed error channels.
- Keep fused mmap internals behind `PdfEngine`.

## Releases and Maven Central

Public coordinates are:

```scala
libraryDependencies += "io.github.adrielc" %% "zio-pdf" % version
```

Tags matching `v*` run the full tests, examples, package audit, external-consumer proof, and signed Maven Central publication before GitHub creates a release. Publication requires repository secrets named `PGP_SECRET`, `PGP_PASSPHRASE`, `SONATYPE_USERNAME`, and `SONATYPE_PASSWORD`. The workflow fails if any are missing.
