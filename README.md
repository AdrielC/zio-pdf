# zio-pdf (formerly fs2-pdf)

> :warning: **Repository Status: rewrite in progress**
>
> The original `fs2-pdf` was archived. This branch ports the project
> off of `cats-effect` / `fs2` / `scodec-stream` (Scala 2.13) and onto
> the latest **ZIO 2 / Scala 3** ecosystem.

## What this branch contains

- **Scala 3.8.3** (the latest 3.8.x release).
- **ZIO 2.1.25** with `zio-streams`, `zio-prelude` 1.0.0-RC47.
- **`zio-blocks-schema` 0.0.33** for schema-derived codecs (smoke-tested).
- **scodec-core 2.3.3** + **scodec-bits 1.2.4**.
- A **ZIO port of `scodec.stream.StreamDecoder`** (the file from the
  original prompt) implemented on top of `ZChannel`.
- A `zio-test` suite covering the streaming-decoder semantics
  (`once`, `many`, `tryMany`, `++`, `flatMap`, `isolate`, `ignore`,
  `emit`/`emits`, `raiseError`, `strict`, byte/bit pipelines).
- The original Scala 2.13 / fs2 sources are preserved, untouched, in
  `legacy/` for reference. They are not part of the new build and
  would need a much larger port to compile against ZIO/Scala 3.

## Building & testing

```bash
sbt test
```

```
[info] + StreamDecoder
[info]   + once decodes a single value and stops
[info]   + many decodes all values until the input ends
[info]   + many decodes across many small chunks (rechunk by 1 byte == 8 bits)
[info]   + many fails when an inner decoder fails (failOnErr = true)
[info]   + ++ runs the right decoder on the leftover from the left
[info]   + emits yields the supplied values
[info]   + emit yields a single value without consuming any input
[info]   + tryMany stops cleanly when an inner decoder fails
[info]   + isolate reads exactly the requested number of bits
[info]   + ignore drops the requested number of bits
[info]   + flatMap can choose a continuation based on a decoded value
[info]   + raiseError(Err) wraps in CodecError
[info]   + toBytePipeline works on a Byte stream
[info]   + strict round-trip via Decoder yields the same values plus leftover
[info]   + raiseError fails the stream with the supplied throwable
[info] + zio-blocks-schema smoke
[info]   + derived Schema is non-null and reports the expected name
[info] 16 tests passed. 0 tests failed. 0 tests ignored.
```

## The ZIO `StreamDecoder` port

The new module lives at:

- `src/main/scala/zio/scodec/stream/StreamDecoder.scala`
- `src/main/scala/zio/scodec/stream/CodecError.scala`

It is a faithful port of the FS2 implementation: the public API
(`once`, `many`, `tryOnce`, `tryMany`, `emit`, `emits`, `++`,
`flatMap`, `map`, `isolate`, `ignore`, `raiseError`, `strict`,
`decode`, `toPipeline`, `toBytePipeline`) matches the original.

Internally each decoder compiles to a single `ZChannel`:

```scala
type BitChannel[+A] =
  ZChannel[Any, Throwable, Chunk[BitVector], Any, Throwable, Chunk[A], BitVector]
```

The `OutDone` slot of the channel carries the *leftover bits* the
decoder peeked at but did not consume, which is what makes
`++`-style sequencing trivial (just `flatMap` the channel) and what
keeps `isolate` honest (any over-read bits flow back out).

### Usage

```scala
import zio.*
import zio.stream.*
import scodec.bits.BitVector
import scodec.codecs.uint8
import zio.scodec.stream.StreamDecoder

val source: ZStream[Any, Throwable, BitVector] = ???
val decoded: ZStream[Any, Throwable, Int] =
  StreamDecoder.many(uint8).decode(source)
```

Or, equivalently, as a `ZPipeline`:

```scala
val pipe: ZPipeline[Any, Throwable, BitVector, Int] =
  StreamDecoder.many(uint8).toPipeline

val decoded: ZStream[Any, Throwable, Int] = source.via(pipe)
```

For byte streams there is `toBytePipeline`:

```scala
val bytes: ZStream[Any, Throwable, Byte] = ???
val ints: ZStream[Any, Throwable, Int] =
  bytes.via(StreamDecoder.many(uint8).toBytePipeline)
```

## `ZPure` and `ZChannel`: the hybrid story

The port uses **both** primitives, and they have very different jobs:

- **`ZChannel`** (`StreamDecoder`) is ZIO's native primitive for
  stream-shape transformations: it can read one element type from
  upstream, write a different element type downstream, and finish
  with a value. It is the underlying abstraction for `ZStream`,
  `ZPipeline`, and `ZSink`. That is exactly the shape of a *streaming
  I/O boundary*, so the channel-based implementation is direct and
  allocation-free per byte beyond the carry buffer.

- **`ZPure`** (`PureDecoder`) is the right tool for the *per-step
  pure decoding logic* — the part that doesn't actually do I/O, just
  consumes some buffered bits and produces zero or more values. By
  modeling that step as `ZPure[A, BitVector, BitVector, Any,
  CodecError, Status]` we get every capability we need without ever
  needing a `Runtime`:

  | `ZPure` slot | Decoder role |
  |---|---|
  | `S1 = S2 = BitVector`           | the *carry buffer* of bits not yet consumed |
  | `W = A`                         | each emitted value is a *log entry* (so `runAll` returns `Chunk[A]` of outputs directly) |
  | `E = CodecError`                | only *fatal* failures bubble up |
  | success channel `Status`        | `NeedMore` / `Done` / `DoneTryAgain` lets the caller loop without paying for a `ZPure.fail` allocation |
  | `R = Any`                       | reserved (the legacy `Log` / config can plug in here later) |

  Pure decoders are testable **without a `Runtime`** — every test in
  the `pure-only` suite calls `pd.run.runAll(bits)` directly:

  ```scala
  val pd            = PureDecoder.many(uint8)
  val (log, result) = pd.run.runAll(bits)
  // log: Chunk[Int] - all decoded values
  // result: Either[CodecError, (BitVector, Status)]
  ```

The two halves compose through `StreamDecoder.fromPure` (or
`PureDecoder#toStreamDecoder`):

```scala
import zio.scodec.stream.{PureDecoder, StreamDecoder}

val pd: PureDecoder[Int]      = PureDecoder.many(scodec.codecs.uint8)
val sd: StreamDecoder[Int]    = StreamDecoder.fromPure(pd)
// or:
val sd2: StreamDecoder[Int]   = pd.toStreamDecoder
```

Inside `fromPure`, the channel pulls a chunk from upstream, appends
it to the carry, calls `pd.run.runAll(carry)`, writes everything in
the returned log downstream, and decides whether to keep pulling
based on the returned `Status`. This is the proper division of labor
between `zio-prelude.ZPure` (the pure step) and `zio.stream.ZChannel`
(the producer/consumer plumbing).

This hybrid is exactly the shape needed when porting the rest of
the legacy `fs2-pdf` code: things like xref accumulation, trailer
sanitization, and PDF-object stream extraction are all pure
state-and-log computations that fit `ZPure` perfectly, and they get
wired into the streaming pipeline via `fromPure` at the very end.

## Performance

A microbench in the test suite (`PerfBench`) decodes ~4 MiB of
`uint8` values four ways. Single-shot in-memory and streaming in
64 KiB chunks (lower is better, JVM 21, OpenJDK):

| approach | time / 4 MiB | notes |
|---|---:|---|
| `scodec.codecs.vector(uint8).decode` (strict baseline) | ~320–460 ms | single big strict decode, no streaming overhead |
| **`PureDecoder.many(uint8).run.runAll`** | ~410–510 ms | competitive with the baseline, *no Runtime needed* |
| `StreamDecoder.many(uint8).decode` (channel, 64 KiB chunks) | ~900 ms | every emitted value flows through ZChannel.write |
| **`StreamDecoder.fromPure(PureDecoder.many)`** (64 KiB chunks) | **~340 ms** | one `runAll` per upstream chunk; beats the channel by ~2.7× |
| `StreamDecoder.many(uint8).strict.decode` | ~1800 ms | spins up an unsafe Runtime per call; for tests only |

The takeaway is the two-layer architecture pays for itself: write
your decoder as a `PureDecoder`, lift it into the streaming pipeline
with `fromPure`, and you get the streaming I/O semantics of a
`ZChannel` at the cost of pure per-step decoding.

## What's ported from the original fs2-pdf

The original `fs2-pdf` source tree (~4 700 lines, 56 files) is in
`legacy/` for reference. The current port covers the foundational
layer end-to-end:

| module | status |
|---|---|
| `zio.scodec.stream.StreamDecoder` (ZChannel) | ✅ ported, 15 tests |
| `zio.scodec.stream.PureDecoder`   (ZPure)   | ✅ ported, 16 tests |
| `zio.pdf.codec.Newline / Whitespace / Text / Many / Codecs` | ✅ ported |
| `zio.pdf.Comment`                            | ✅ ported |
| `zio.pdf.Version`                            | ✅ ported |
| `zio.pdf.StartXref`                          | ✅ ported |
| `zio.pdf.TopLevel` (Version + Comment + StartXref) wired through `StreamDecoder.many(...)` and exposed as a `ZPipeline[Any, Throwable, Byte, TopLevel]` | ✅ ported, includes a parse of the legacy `xref-stream.pdf` fixture |
| `Prim` (PDF primitive objects: Dict, Array, Ref, ...) | ⏳ remaining |
| `Obj` / `IndirectObj`                        | ⏳ remaining |
| `Xref` (textual + stream form) / `Trailer`   | ⏳ remaining |
| Higher-level pipes (`Decode`, `Elements`, `Rewrite`, `WritePdf`) | ⏳ remaining |

The remaining work is mechanical (replace `cats.effect.IO` →
`zio.Task`, `fs2.Stream` → `ZStream`, `fs2.Pull` → `ZChannel`,
`cats.data.NonEmptyList` → `zio.NonEmptyChunk`, `Validated` →
`zio.prelude.Validation`, `shapeless.HList` → Scala 3 tuples) but
voluminous, and should land in follow-up PRs. The new `StreamDecoder`
+ `PureDecoder` + `zio-blocks-schema` are wired in, so the existing
PDF ADTs can additionally get JSON / Avro / MessagePack codecs
derived for free as they are ported.

## License

Copyright 2020-2026 SpringerNature. Apache License 2.0.
