# Contributing to zio-pdf

This GitHub repository is the public source and release project. Every change intended for Maven Central must build without private resolvers, private artifacts, or credentials.

## Workflow

1. Clone the repository:

   ```bash
   git clone https://github.com/AdrielC/zio-pdf.git
   ```

2. Branch from `main`.
3. Keep byte streams incremental. Any collecting helper must state and enforce its bound or return a type whose full materialization is the method's explicit contract.
4. Put new parser and inspection work in the ZIO-free `kyo-pdf` core. Keep
   ZIO compatibility in `kyo-pdf-zio`; the legacy `PdfEngine`, `PdfStream`,
   and `PdfIO` surface remains available during the writer/layout migration.
5. Run:

   ```bash
   npm ci
   sbt -batch ";kyoPdf/test;kyoPdfZio/test;root/test;scalaJs/test;bench/test"
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

- Use Scala 3 and Kyo idioms in `kyo-pdf-core`; ZIO belongs only in the
  compatibility module and legacy implementation.
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

The Kyo modules inherit the same public organization and source build version.
Build local dependencies with `sbt -batch ';kyoPdf/publishLocal;kyoPdfZio/publishLocal'`.
The release workflow publishes the JVM `kyo-pdf`, `kyo-pdf-zio`, and
`kyo-pdf-pipeline` modules, the Native Kyo core, and JVM/Scala.js `zio-pdf`.
The Native Policy/pipeline module and its five public-source Schema support
modules are also published. Verify upstream source provenance with
`python3 scripts/verify-native-provenance.py` and link an independent Native
consumer with `bash scripts/verify-native-policy-consumer.sh`.

Kyo packages use `io.github.adrielc.kyopdf` and `io.github.adrielc.kyopdf.zio`.
Consumers of earlier Kyo pre-release artifacts must update imports and the
dependency organization and recompile. The package move changes source and
binary compatibility for those artifacts; keep this migration explicit in
release notes.

Tags matching `v*` run the full tests, examples, Gotenberg tool tests, package audits, external-consumer proofs, and signed Maven Central publication before GitHub creates a release. All twelve artifacts must resolve from Maven Central, and both JVM and Native Policy consumers must run from those published dependencies. Publication requires repository secrets named `PGP_SECRET`, `PGP_PASSPHRASE`, `SONATYPE_USERNAME`, and `SONATYPE_PASSWORD`. The workflow fails if any are missing.
