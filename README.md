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

## Why use `ZChannel` (and not `ZPure`)

`ZChannel` is ZIO's primitive for *stream-shape* transformations: it
can read one element type from upstream, write a different element
type downstream, and finish with a value. It is the underlying
abstraction for `ZStream`, `ZPipeline`, and `ZSink`. That is exactly
the shape of a streaming decoder, so the implementation is direct and
fast (no allocation per byte beyond the carry buffer).

`ZPure` is the right choice for *pure* state/log/error/reader
computations that produce a single value, but it does not natively
model the producer/consumer shape of a streaming decoder. Mixing the
two would mean wrapping `ZPure` runs in a channel, which buys no
power but adds an allocation per step. So the port stays on
`ZChannel` and is happy to lean on `zio-prelude.ZPure` for
*pure helpers* (e.g. accumulating xref tables) when the legacy code
is migrated.

## Roadmap

The original `fs2-pdf` source tree (~4 700 lines, 56 files) is in
`legacy/`. Migrating it requires:

1. Replacing `cats.effect.IO` with `zio.Task` / `zio.IO`.
2. Replacing `fs2.Stream` / `fs2.Pipe` / `fs2.Pull` with ZIO's
   `ZStream` / `ZPipeline` / `ZChannel`.
3. Replacing `cats.data.NonEmptyList` / `Validated` with the
   equivalents from `zio.NonEmptyChunk` and `zio.prelude.Validation`.
4. Replacing every `scodec.stream.StreamDecoder` callsite with the
   new `zio.scodec.stream.StreamDecoder`.
5. Replacing the few `scodec` 1.x APIs that moved/changed in 2.x
   (mostly cosmetic).
6. Optionally lifting the existing PDF data ADTs (`Prim`, `Obj`,
   `IndirectObj`, `Xref`, `Trailer`, `Pages`, etc.) onto
   `zio-blocks-schema` so they get JSON / Avro / MessagePack codecs
   for free.

The `StreamDecoder` port and the new build are the foundation. The
remaining work is mechanical but voluminous and should be done in
follow-up PRs to keep diffs reviewable.

## License

Copyright 2020-2026 SpringerNature. Apache License 2.0.
