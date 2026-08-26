# Production invariants

`zio-pdf` separates raw structural streaming from bounded high-level decoding.
The following invariants apply to every public API and release artifact.

## Byte ownership and bounds

- A caller-owned `ZStream[Byte]` is lazy and may be arbitrarily large.
- Raw scanning, hashing, copying, and storage never collect the complete input.
- Any operation that materializes bytes must use a named bound and must reject
  the first byte beyond that bound before returning a value.
- APIs that retain a complete decoded document timeline enforce a separate
  materialized-document bound; streaming APIs do not inherit that restriction.
- Byte counts and offsets are non-negative `Long` values. Narrowing to `Int` is
  allowed only after enforcing an `Int`-sized bound.
- Decompression has an independent decoded-byte limit. Input size alone is not
  protection against a decompression bomb.
- Buffered concurrency has an explicit upper bound. No stage may create an
  unbounded queue of byte chunks or decoded objects.

## Resources and interruption

- Files, streams, and decoder sessions remain scoped for their entire use.
- Early downstream termination and fiber interruption close acquired resources.
- Public effectful callbacks execute through ZIO stream operators. Production
  code must not call `Runtime.unsafe.run` inside synchronous callbacks.

## Errors and output integrity

- Expected parse, policy, bounds, I/O, and encoding failures use typed errors.
- A writer either emits a structurally consistent PDF or fails before emitting
  a trailer that would claim false lengths or offsets.
- Declared stream lengths are checked against bytes actually emitted.
- Cross-reference offsets are derived in physical output order and verified by
  an independent PDF implementation in CI.

## Release truthfulness

- Coordinates are described as published only after Maven Central read-back.
- Stable, experimental, JVM-only, and Scala.js APIs are identified explicitly.
- Artifact audits inspect the exact JAR and POM that release automation will
  publish and must fail closed when their own inspection commands fail.
- Structurally streaming, logically supported, and physically exercised sizes
  are reported as separate claims.
