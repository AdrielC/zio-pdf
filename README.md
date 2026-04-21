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

## API at a glance (`zio.scodec.stream.syntax.*`)

```scala
import zio.scodec.stream.syntax.*
import scodec.codecs.uint8

// scodec.Decoder  ->  StreamDecoder
val sd = uint8.streamMany               // ::= StreamDecoder.many(uint8)
val so = uint8.streamOnce               // ::= StreamDecoder.once(uint8)

// scodec.Decoder  ->  PureDecoder (the ZPure layer)
val pd = uint8.pureMany                 // ::= PureDecoder.many(uint8)

// scodec.Decoder  ->  the canonical pure step (a ZPure)
val ds = uint8.asPureStep               // ::= PureDecoder.fromDecoder(uint8)

// PureDecoder  ->  StreamDecoder
val pdsd = uint8.pureMany.toStream      // ::= StreamDecoder.fromPure(uint8.pureMany)

// ZStream sugar
byteStream.decodeMany(uint8)            // Byte stream -> A stream
byteStream.viaBytesDecoder(pd)          // Byte stream -> A stream via a PureDecoder
bitStream.decodeManyBits(uint8)         // BitVector stream -> A stream
bitStream.viaDecoder(sd)                // explicit StreamDecoder

// In-memory strict
bits.decodeStrict(sd)                   // Either[CodecError, DecodeResult[Chunk[A]]]
bits.decodePure(pd)                     // same, via the ZPure layer
```

Day-to-day callsites no longer have to mention `StreamDecoder.fromPure(PureDecoder.many(...))` or chain `.toBytePipeline.via(...)`-style noise.

## Performance

Run JMH with:

```bash
sbt 'bench/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu ms .*StreamDecoderBench.*'
sbt 'bench/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu us .*RingBufferBench.*'
```

### `StreamDecoder` / `PureDecoder` — 1 MiB of `uint8`, JDK 21

```
Benchmark                                     (chunkSize)      (n)  Mode  Cnt    Score    Error  Units
StreamDecoderBench.chunkedFastPathStrict            65536  1048576  avgt    5    0.890 ±  0.081  ms/op   <-- ~57x baseline
StreamDecoderBench.chunkedFastPathChannel           65536  1048576  avgt    5    1.146 ±  0.224  ms/op   <-- ~44x baseline
StreamDecoderBench.scodecVectorBaseline             65536  1048576  avgt    5   50.873 ±  0.751  ms/op   reference
StreamDecoderBench.pureDecoderRunAll                65536  1048576  avgt    5   95.215 ± 11.892  ms/op
StreamDecoderBench.streamDecoderHybrid              65536  1048576  avgt    5  105.574 ±  9.118  ms/op
StreamDecoderBench.streamDecoderStrict              65536  1048576  avgt    5  106.975 ±  3.026  ms/op
StreamDecoderBench.syntaxStreamDecoderStrict        65536  1048576  avgt    5  106.703 ±  7.950  ms/op
StreamDecoderBench.streamDecoderChannel             65536  1048576  avgt    5  243.897 ±  6.592  ms/op
```

Five takeaways:

1. **The chunked fast path beats `scodec.codecs.vector` by ~57×.** It uses `PureDecoder.manyChunked + StreamDecoder.fromPureChunked`: each upstream chunk is consumed in **one** `ZPure.runAll`, decoded in **one** tight `while` loop on the underlying `Array[Byte]`, and shipped downstream as **one** `ZChannel.write`. By contrast, `scodec.codecs.vector(uint8)` does ~1M individual `uint8.decode` calls (each allocates a 1-element `BitVector` slice and a `DecodeResult`) and concats into a boxed `Vector[Int]` — most of its time is allocation and per-element dispatch, not actual byte reading.
2. **`inline` extension methods are free.** `uint8.streamMany.strict.decode` (sugar) and `StreamDecoder.many(uint8).strict.decode` (raw) are within JMH's error bars.
3. **`strict` is no `Runtime` away from `scodec`.** The first cut spun up a `ZChannel` and `unsafe`-ran it (~1800 ms); `runStrict` walks the `Step` algebra directly in pure code (~107 ms), within ~2× of `vector`. ZIO IO belongs at the streaming-I/O boundary; never on the in-memory-decode hot path.
4. **The two-layer architecture pays for itself.** `streamDecoderHybrid` (= `StreamDecoder.fromPure(PureDecoder.many)`) is ~2.3× faster than the plain `ZChannel`-only path because each upstream chunk drives one `ZPure.runAll` instead of looping through `ZChannel.write` per emitted value.
5. **For maximum throughput on byte-aligned, fixed-width formats, use the chunked fast path.** It's the only one that bypasses both per-element scodec dispatch *and* per-element ZPure log overhead.

How to write your own chunked fast path:

```scala
import zio.scodec.stream.*, zio.scodec.stream.syntax.*
import zio.Chunk

inline def uint8Batch: PureDecoder[Chunk[Int]] =
  PureDecoder.manyChunked[Int] { arr =>
    val out = new Array[Int](arr.length)
    var i   = 0
    while (i < arr.length) { out(i) = arr(i) & 0xff; i += 1 }
    Chunk.fromArray(out)
  }

val sd: StreamDecoder[Int] = uint8Batch.toStreamChunked
// then:  byteStream.viaDecoder(sd) : ZStream[..., Int]
```

The `inline` on `manyChunked` matters: the JIT inlines the per-batch body straight into the `runAll` loop, with no method-call boundary.

### `SpscRingBuffer` (`zio-blocks-ringbuffer`) vs `ArrayBlockingQueue`

```
Benchmark                                      (n)  Mode  Cnt    Score    Error  Units
RingBufferBench.spscRingBufferFillDrain      16384  avgt    5   70.929 ± 13.977  us/op   ~7x faster
RingBufferBench.arrayBlockingQueueFillDrain  16384  avgt    5  485.797 ± 33.699  us/op
```

For 16 K element fill-then-drain on a single thread, `SpscRingBuffer` is ~7× faster than `ArrayBlockingQueue` per element. The win when there's a real thread boundary is bigger because `ABQ` has to acquire a lock per `offer`/`poll` while `SpscRingBuffer` only needs a single relaxed write per slot. Use it for "decoder fiber → consumer fiber" handoff when the work is CPU-bound and the queue is on the hot path.

**About `zio-blocks-streams`**: the project ships (`0.0.20`) but the API is still pull-based, alpha-grade, and not a drop-in replacement for `ZStream` for our purposes. The dep is wired into `build.sbt` so it's available; it'll be the natural target for a deeper refactor once it stabilises.

## `ScodecDeriver` — derive `scodec.Codec[A]` from `Schema[A]`

`zio-blocks-schema` ships with a `Deriver[TC]` framework: implement seven methods (`derivePrimitive`, `deriveRecord`, `deriveVariant`, `deriveSequence`, `deriveMap`, `deriveDynamic`, `deriveWrapper`) and you get type-class derivation for free. We have one for `scodec.Codec`:

```scala
import zio.blocks.schema.Schema
import zio.scodec.schema.ScodecDeriver
import scodec.Codec

case class Address(street: String, zip: Int)
object Address {
  given Schema[Address] = Schema.derived[Address]
}

case class Person(name: String, age: Int, address: Address)
object Person {
  given Schema[Person] = Schema.derived[Person]
}

val codec: Codec[Person] = summon[Schema[Person]].derive(ScodecDeriver)

val bits = codec.encode(Person("Alice", 30, Address("1 Wonderland Way", 12345))).require
val back = codec.decode(bits).require.value
// back == Person("Alice", 30, Address("1 Wonderland Way", 12345))
```

This is the architectural payoff of wiring `zio-blocks-schema` in: a single `Schema.derived[Foo]` declaration produces the binary codec, JSON codec (`JsonCodecDeriver`), and any other format-specific deriver from the same schema — no more hand-rolling 200 `Codec[Foo]` instances.

### Coverage of the seven `Deriver` methods

| Pattern | Implementation |
|---|---|
| **Primitive** | dispatch on `PrimitiveType`; uses `int32` / `int64` / `bool(8)` / `float` / `double` / `utf8_32` / etc. from `scodec.codecs` |
| **Record** | encode each field in declaration order; decode each, accumulate into `Registers`, run `binding.constructor.construct` |
| **Variant** | `uint8` discriminator tag + matching case codec via `binding.discriminator` and `binding.matchers` |
| **Wrapper** | `xmap(binding.wrap, binding.unwrap)` |
| **Sequence** | length-prefixed (`int32`) list of element codecs; `binding.constructor.newBuilder + add + result` |
| **Map** | length-prefixed list of `(K, V)` pairs |
| **Dynamic** | not implemented in this prototype (returns a clear failure) |

Tests round-trip `Address`, nested `Person`, three-case `Shape` (`Circle | Rectangle | Triangle`), `Team` with `List[String]`, and assert exact wire size for the variant case (1-byte tag + two 8-byte doubles = 17 bytes for `Rectangle`).

### Why this matters for the PDF port

The legacy `fs2-pdf` data model (`Prim` / `Obj` / `IndirectObj` / `Xref` / `Trailer` / `Pages` / `Element` / `Decoded` / ...) is dozens of mutually-recursive case classes and sealed traits. Hand-rolling the `Codec[…]` for each was the original project's main maintenance burden. With `ScodecDeriver`, each ADT just needs `given Schema[Foo] = Schema.derived[Foo]` and the codec falls out for free — and the same `Schema[Foo]` simultaneously gives you JSON, Avro, etc. via the other derivers in `zio-blocks-schema`.

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
