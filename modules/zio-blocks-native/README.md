# Native Schema runtime for Kyo PDF

The Native pipeline uses the public ZIO Blocks v0.0.56 sources at
[`b4523963f2981c15f15d0afa05b33a1ff0cf38a9`](https://github.com/zio/zio-blocks/tree/b4523963f2981c15f15d0afa05b33a1ff0cf38a9).
Upstream publishes that version for JVM and Scala.js; its Native releases stop
at v0.0.14. Kyo PDF publishes this Native backport under its own
`io.github.adrielc:kyo-pdf-blocks-*` coordinates and release version. These are
Kyo PDF support artifacts, not official ZIO Blocks releases.

Chunk, TypeId, Maybe, Markdown, Schema and TOON compile as separate modules so that
Schema derivation macros remain available to Native consumers. Their public
`zio.blocks` packages are preserved, and the pipeline uses the same sources and
JSON and TOON formats on JVM and Native. The support artifacts are transitive dependencies;
consumers need only add `kyo-pdf-pipeline_native0.5_3`.

`UPSTREAM.json` lists every copied source, its original SHA-256 and any adapted
SHA-256. The Native platform uses upstream's portable Scala.js JSON reader,
writer, primitive registers and chunk helpers. The adaptations are:

- Platform flags identify Native, with both `isJVM` and `isJS` false.
- TypeId and binding reflection are unavailable, as on Scala.js. Derive schemas
  for nominal types at compile time instead.
- Structural bindings produce a Native-specific compile-time error, using the
  upstream non-experimental macro fallback. Consumers do not need `-experimental`.
- The TOON codec forwards optional-array decoding, descends into optional nested
  records, rejects duplicate record fields, and distinguishes quoted strings
  containing colons from record fields. Dynamic fields use the standard nested
  record and counted-array formatting. The same public sources and fixes compile
  for JVM as `kyo-pdf-blocks-schema-toon_3` and for Native as
  `kyo-pdf-blocks-schema-toon_native0.5_3`. They support ordinary automatic codec
  derivation; recipes contain no format-specific field adapters. Other shared
  sources remain unchanged.

The portable IDN helper retains upstream Scala.js behavior: it does not translate
international domain names to ASCII. This backport is tested for Policy,
SchemaExpr and pipeline JSON/TOON transport; it does not add JVM reflection or other
JVM services to Native. Do not combine these artifacts with another Native
runtime defining the same `zio.blocks` packages.

Verify provenance against the pinned public archive:

```sh
python3 scripts/verify-native-provenance.py
```

The original copyright headers are retained. See LICENSE and NOTICE.
