# volga-core (vendored)

Vendored copy of [tofu-tf/volga](https://github.com/tofu-tf/volga) `modules/core`, maintained in-tree for zio-pdf.

The helper remains a separate compilation unit for macros. Its classes and sources
are bundled into the JVM `zio-pdf` artifact; no separately published helper is
required. JVM graph and ingest adapters are excluded from the Scala.js parser.

## Why in-tree

- **Scala 3.8 SMC fixes** in `volga.syntax.parsing` (`Var.scala`, `MParsing.scala`) — tuple destructuring and `NoType` RHS inference
- No git submodule / clone friction for contributors or CI
- Typed Scala tuple products and tensor applications alongside the existing Nat
  PROP universe, including product-returning sources and exact tuple reconstruction
- Associator direction and reduced product evidence in macro generation, covered by
  typed three- and four-input joins rather than arity-only checks
- Room to evolve wiring/SMC alongside `zio.pdf.arrow` and the typed PDF arrow adapter
  without upstream release lag

## Upstream

Based on volga @ `92eb571` (Scala 3.8 SMC parsing patch). When syncing upstream, copy from `tofu-tf/volga/modules/core` and re-apply zio-pdf-specific fixes if needed.

## License

Apache 2.0 (same as upstream volga and zio-pdf).
