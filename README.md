# zio-pdf (formerly fs2-pdf)

> :warning: **Repository Status: rewrite in progress**
>
> The original `fs2-pdf` was archived. This branch ports the project
> off of `cats-effect` / `fs2` / `scodec-stream` (Scala 2.13) and onto
> the latest **ZIO 2 / Scala 3** ecosystem.

## What this branch contains

- **Scala 3.8.3** (the latest 3.8.x release).
- **ZIO 2.1.25** with `zio-streams`, `zio-prelude` 1.0.0-RC47.
- **Kyo 1.0-RC1** (`kyo-data`, `kyo-kernel`, `kyo-prelude`, `kyo-core`)
  for the algebraic-effect runtime used by the Scan algebra.
- **`zio-blocks-schema` 0.0.33** for schema-derived codecs (smoke-tested).
- **scodec-core 2.3.3** + **scodec-bits 1.2.4**.
- A **ZIO port of `scodec.stream.StreamDecoder`** (the file from the
  original prompt) implemented on top of `ZChannel`.
- A **Graviton Scan algebra** under `zio.pdf.scan` -- a free symmetric
  monoidal category over a small primitive set (`Map`, `Filter`,
  `Take`/`Drop`, `Fold`, `Hash`, `CountBytes`, `BombGuard`, `FastCDC`,
  `FixedChunk`), interpreted into Kyo's `Poll`/`Emit`/`Abort` triple.
  See [Graviton Scan](#graviton-scan) below.
- A `zio-test` suite covering the streaming-decoder semantics
  (`once`, `many`, `tryMany`, `++`, `flatMap`, `isolate`, `ignore`,
  `emit`/`emits`, `raiseError`, `strict`, byte/bit pipelines) and the
  Scan algebra (composition, fusion, stack-safety, fanout/choice,
  hash, count, bomb-guard, take/drop, fold).
- The original Scala 2.13 / fs2 sources are preserved, untouched, in
  `legacy/` for reference. They are not part of the new build and
  would need a much larger port to compile against ZIO/Scala 3.

## Graviton Scan

A scan is a morphism `FreeScan[I, O]` in a free symmetric monoidal
category whose primitives are `ScanPrim[I, Out]`. Each primitive carries
its output cardinality *statically* as a subtype:

- `Null`        -- zero outputs (literally `scala.Null`)
- `One[O]`      -- exactly one output (opaque alias for `O`, no boxing)
- `NonEmpty[O]` -- one or more outputs (opaque alias for `O | Chunk[O]`)

with `One[O] <: NonEmpty[O]` so the compiler can fuse `One`-emitting
chains into a single `I => O`. The runtime execution model is three Kyo
effects in one row: `Poll[I]` for pulling inputs, `Emit[O]` for pushing
outputs, and `Abort[ScanSignal]` for typed completion (`ScanDone[O, E]`)
with leftover. State lives in `Var[S]`; resources, when needed, in
`Scope`.

```scala
import zio.pdf.scan.*

val pipeline: FreeScan[Byte, kyo.Chunk[Byte]] =
  Scan.bombGuard(maxBytes = 1L << 30) >>>
  Scan.fastCdc(min = 4 * 1024, avg = 16 * 1024, max = 64 * 1024)

val (signal, chunks) = Scan.runDirect(pipeline, fileBytes)
//   ^ ScanDone.Success | Stop | Failure   ^ all emitted CDC chunks
```

The same scan can be executed through Kyo's effect machinery with
`Scan.runKyo`. The interpreter (`SinglePassInterp`) flattens left-nested
`AndThen` spines once, fuses any pure-function chain into a single
`I => O`, and lowers the rest to a stack-safe stepper. Kyo 1.0 supplies
the `Poll`/`Emit`/`Abort` plumbing; the layer order is `Abort` innermost
(catches `ScanSignal`), `Emit` middle (collects outputs), `Poll`
outermost (drives the program from a `Chunk[I]`). The leftover from the
abort payload is appended to what `Emit` collected -- no re-emission, no
broken Stream/Poll symmetry.

### Combinators

The full Arrow / ArrowChoice surface is available as extension methods
on `FreeScan`:

| Family       | Operators |
|---           |---        |
| Category     | `>>>`, `<<<`, `andThen`, `compose`, `FreeScan.id` |
| Profunctor   | `map`, `contramap`, `dimap` |
| Strong arrow | `first`, `second`, `***`, `&&&`, `Scan.diag`, `Scan.fst`, `Scan.snd`, `Scan.swap`, `keepFirst`, `keepSecond` |
| Choice arrow | `left`, `right`, `+++`, `\|\|\|`, `Scan.mirror`, `Scan.merge`, `Scan.injectLeft`, `Scan.injectRight`, `Scan.test` |
| Glue         | `Scan.const`, `Scan.void`, `drainLeft`, `Scan.arr`, `Scan.lift` |

Everything except the leaf primitives is derived from `arr`, `>>>`,
`&&&` and `|||`, so `Fusion.tryFuse` automatically collapses any pure
sub-spine -- including pure tuple shuffling like `swap`, `mirror`, `fst`
and `snd` -- into a single `I => O` on the hot path.

Code lives under [`src/main/scala/zio/pdf/scan/`](src/main/scala/zio/pdf/scan/);
tests under [`src/test/scala/zio/pdf/scan/`](src/test/scala/zio/pdf/scan/)
(`ScanSpec` for the primitive properties; `AdvancedCompositionSpec` for
deep `>>>` / `&&&` / `|||` composition and the Graviton-style ingest
patterns; `ArrowKitchenSinkSpec` for the full Arrow / ArrowChoice
surface and a single pipeline that uses *every* combinator at once;
`ScanPerfBench` for the in-test perf snapshot).

### Performance

The fused `Scan.runDirect` path beats `scodec.codecs.vector(uint8)`'s
strict baseline by roughly 3-4x on the canonical "decode 1 MiB through a
4-stage pure pipeline" workload, despite doing strictly more work (the
scan also runs three arithmetic stages on each decoded byte; the scodec
baseline just decodes them):

```
=== scan vs scodec on 1048576 bytes (lower is better, in-test bench) ===
  [scodec.codecs.vector(uint8).decode                ]   111 ms / iter
  [Scan.runDirect (fused, 4 maps)                    ]    24 ms / iter
  [Scan.runDirect (unfused, 4 maps + filter)         ]   204 ms / iter
  [Scan.runKyo  (fused, 4 maps, N=1024)              ]     7 ms / iter
  [hand-coded while loop (reference)                 ]     2 ms / iter
  ratio fused/scodec = 0.35x
```

JMH numbers from `bench/Jmh/run -p n=1048576 zio.pdf.scan.bench.ScanBench`
agree:

```
Benchmark                        (n)  Mode  Cnt    Score     Error  Units
ScanBench.handCoded          1048576  avgt    3    0.937 ±   0.672  ms/op
ScanBench.scanFusedDirect    1048576  avgt    3   23.525 ±  35.059  ms/op
ScanBench.scanUnfusedDirect  1048576  avgt    3  179.283 ± 119.168  ms/op
ScanBench.scodecBaseline     1048576  avgt    3   63.310 ±  29.787  ms/op
```

The fast path is `Fusion.tryFuse` recognising that every node in the
spine is `Arr` or `Prim(ScanPrim.Map)` and collapsing the chain to a
single `I => O`. The runner then becomes one `builder += f(_)` per
input byte -- no `Stepper`, no `StepEffect`, no per-stage dispatch.

The unfused lane (4 maps + a `Filter`) measures the cost when fusion is
not possible: the driver routes each byte through the per-stage stepper
and pays one `StepEffect` allocation per element.

The Kyo lane pays a fixed per-element suspension cost from
`Poll`/`Emit`/`Abort`, which dominates trivial workloads but stays flat
under bigger per-stage work; it's intentionally not part of the JMH
suite (the in-test bench measures it on a smaller payload so the
result is visible without dominating the run).

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

### Head-to-head vs fs2 (the library we ported away from)

A separate sbt subproject `bench-fs2/` wires in `fs2 3.13.0` + `fs2-scodec 3.13.0` (the folded-in successor to the now-archived `scodec-stream`) + `cats-effect 3.7.0`. The transitive cloud lives only in that module and never touches the main project.

```bash
sbt 'benchFs2/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu ms .*HeadToHeadBench.*'
sbt 'benchFs2/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu us .*PdfDecode.*'
```

Same `scodec.Decoder`, same in-memory bytes, same chunk size — only the streaming library differs.

**4 MiB of `uint8` (synthetic, all elements through the streaming layer):**

```
Benchmark                                          (chunkSize)      (n)  Mode  Cnt     Score     Error  Units
HeadToHeadBench.baseline_scodec_vector                   65536  4194304  avgt    5   223.343 ±  39.800  ms/op   reference
HeadToHeadBench.baseline_zio_PureDecoder_runAll          65536  4194304  avgt    5   430.181 ±  32.729  ms/op
HeadToHeadBench.fs2_StreamDecoder_many                   65536  4194304  avgt    5  2659.758 ± 427.614  ms/op   fs2 streaming
HeadToHeadBench.zio_StreamDecoder_many                   65536  4194304  avgt    5  1031.921 ±  87.637  ms/op   ZIO ZChannel streaming    (~2.6x faster than fs2)
HeadToHeadBench.zio_StreamDecoder_fromPureChunked        65536  4194304  avgt    5     4.232 ±   0.608  ms/op   ZIO chunked fast path     (~628x faster than fs2)
```

**Real PDF top-level decode (the legacy `xref-stream.pdf` fixture):**

```
Benchmark                                                    (chunkSize)  Mode  Cnt    Score     Error  Units
PdfDecodeHeadToHeadBench.fs2_decode_pdf_topLevel                    8192  avgt    5  366.326 ± 138.016  us/op   fs2 + scodec.choice
PdfDecodeHeadToHeadBench.zio_decode_pdf_topLevel                    8192  avgt    5  457.112 ± 120.087  us/op   ZIO + scodec.choice
PdfDecodeHeadToHeadBench.zio_decode_pdf_topLevel_byteStream         8192  avgt    5  439.639 ±  68.776  us/op   ZIO + byte-stream pipe
```

Honest reading:

- **For high-throughput byte-aligned decoding** (one element type, many elements): ZIO's chunked fast path beats fs2 by **~628×**. This is the architectural win — `PureDecoder.manyChunked + StreamDecoder.fromPureChunked` lets the entire batch decoder live inside one inlined while-loop per upstream chunk; fs2 has no equivalent because `scodec-stream`'s `Decode` step is per-element.
- **For the plain ZChannel path vs fs2's Pull path** on the same per-element decoder: ZIO is **~2.6× faster** on a tight `uint8` loop. Same algorithm both sides (we ported it from fs2's source), but `ZChannel` has lower per-step overhead than `Pull` for this shape.
- **For real PDF parsing where the scodec `choice`-decoder body dominates** (~360 µs of decoding work per PDF): the two libraries are within ~25% of each other and **fs2 actually edges us by ~25%**. The streaming library overhead is in the noise once the per-element decoder body itself is expensive — what wins or loses at that point is JIT inlining of the choice arms, not channel vs pull.

So if your workload is "stream a giant binary log of fixed-width records" the chunked fast path is a transformative win. If your workload is "parse a few KiB of nested PDF structure", any modern Scala streaming library is fine and the difference is in the noise.

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

### Perf vs hand-rolled

JMH benchmark on a realistic complex type (`Order` with `Customer`, `List[OrderLine]`, three-case `Payment` variant, plus several primitive fields). Encode + decode 10 K records each way:

```
Benchmark                              (n)  Mode  Cnt   Score    Error  Units
ScodecDeriverBench.handRolledEncode  10000  avgt    5  20.962 ±  1.048  ms/op
ScodecDeriverBench.derivedEncode     10000  avgt    5  24.793 ±  1.205  ms/op   +18%
ScodecDeriverBench.handRolledDecode  10000  avgt    5  28.828 ± 10.173  ms/op
ScodecDeriverBench.derivedDecode     10000  avgt    5  30.933 ±  0.454  ms/op   +7%
```

The deriver carries one `Registers` allocation per record and looks up field positions via the `Reflect.Record`'s pre-computed register layout, but otherwise it's allocation-tight. **For complex application types the overhead is in the noise (5-20%).** Where it would actually matter — tight loops over millions of small fixed-width records — the right tool is the chunked fast path (`PureDecoder.manyChunked + StreamDecoder.fromPureChunked`), which beats hand-rolled `scodec.codecs.vector` by ~57× on `uint8`.

### Why this matters for the PDF port

The legacy `fs2-pdf` data model (`Prim` / `Obj` / `IndirectObj` / `Xref` / `Trailer` / `Pages` / `Element` / `Decoded` / ...) is dozens of mutually-recursive case classes and sealed traits. Hand-rolling the `Codec[…]` for each was the original project's main maintenance burden. With `ScodecDeriver`, each ADT just needs `given Schema[Foo] = Schema.derived[Foo]` and the codec falls out for free — and the same `Schema[Foo]` simultaneously gives you JSON, Avro, etc. via the other derivers in `zio-blocks-schema`.

## What's ported from the original fs2-pdf

The original `fs2-pdf` source tree (~4 700 lines, 56 files) is in
`legacy/` for reference. The full decode + transform + encode
pipeline is now ported:

| module | status |
|---|---|
| `zio.scodec.stream.StreamDecoder` (ZChannel) | ✅ ported, 15 tests |
| `zio.scodec.stream.PureDecoder` (ZPure) | ✅ ported, 16 tests |
| `zio.scodec.schema.ScodecDeriver` (`Deriver[scodec.Codec]`) | ✅ ported, 5 tests |
| `zio.pdf.codec.Newline / Whitespace / Text / Many / Codecs` | ✅ ported |
| `zio.pdf.Comment / Version / StartXref` | ✅ ported |
| `zio.pdf.Prim` (Null/Bool/Number/Name/Str/HexStr/Ref/Array/Dict + helpers) | ✅ ported, 10 round-trip tests |
| `zio.pdf.Obj / IndirectObj` (with `EncodedObj` + `XrefObjMeta`) | ✅ ported |
| `zio.pdf.Trailer / Xref` (textual) | ✅ ported |
| `zio.pdf.XrefStream` (compressed) | ✅ ported |
| `zio.pdf.ObjectStream` (`/Type /ObjStm`) | ✅ ported |
| `zio.pdf.FlateDecode` + `zio.pdf.image.Predictor` (Java) | ✅ ported |
| `zio.pdf.Content` (`Uncompressed` + extractors) | ✅ ported |
| `zio.pdf.TopLevel` (`VersionT / CommentT / StartXrefT / WhitespaceT / IndirectObjT / XrefT`) | ✅ ported |
| `zio.pdf.Decoded` + `zio.pdf.Decode` (full decoder pipeline) | ✅ ported, 3 tests |
| `zio.pdf.FilterDuplicates` (suppress duplicate-numbered objects) | ✅ ported |
| `zio.pdf.Element / Elements / Page / Pages / FontResource / Image / IndirectArray / MediaBox` | ✅ ported |
| `zio.pdf.Part / EncodeMeta / GenerateXref / WritePdf` (encoder pipeline) | ✅ ported, 2 round-trip tests |
| `zio.pdf.Rewrite` (stateful transform + emit Part stream) | ✅ ported |
| `zio.pdf.Pdf / AssemblePdf / ValidatedPdf / AssemblyError` | ✅ ported |
| `zio.pdf.ValidatePdf / ComparePdfs / PdfError / CompareError` | ✅ ported, 3 tests |
| `zio.pdf.PdfStream` (top-level façade: `bits / topLevel / decode / elements / transformElements / validate / compare`) | ✅ ported |
| `Pdf.objectNumbers / pageNumber / streamsOfPage / dictOfPage` (read-only convenience pipes) | ⏳ remaining |
| `WriteLinearized` / `Tiff` (specialised image emitters) | ⏳ remaining |
| Image-test helpers (the legacy `Jar` / `ProcessJarPdf` / `WriteFile`) | ⏳ remaining |

The remaining items are convenience helpers and specialised image
emitters, not foundational architecture. The new layout uses ZIO's
`ZStream` / `ZPipeline` / `ZChannel` throughout; `cats.effect.IO`,
`fs2.Pull`, `cats.data.NonEmptyList`, `cats.data.Validated`, and
`shapeless.HList` are completely gone from the production code.

## Memory-bounded encoding for large attachments

`Part.Obj` carries a fully-materialised `IndirectObj`, which is fine for small/medium objects but blows the heap for large content streams (PDF attachments, embedded images, fonts, font subsets). The encoder also accepts a streaming variant:

```scala
sealed trait Part[+A]
object Part {
  // small / medium objects - materialised upfront
  final case class Obj(obj: IndirectObj) extends Part[Nothing]

  // memory-bounded: payload is a ZStream[Any, Throwable, Byte]
  final case class StreamObj(
    index:   Obj.Index,
    data:    Prim,
    length:  Long,                          // exact byte count, patched into /Length
    payload: ZStream[Any, Throwable, Byte]  // never materialised in full
  ) extends Part[Nothing]

  final case class Meta[A](meta: A)               extends Part[A]
  final case class Version(version: Version)      extends Part[Nothing]
}
```

When the encoder hits a `Part.StreamObj`:
1. Encode the `<num> <gen> obj` header + dict (with `/Length` patched in) + literal `stream\n` — one downstream `ZChannel.write`.
2. Forward the payload `ZStream` chunk-by-chunk by piping its underlying channel into the encoder's downstream — **at most one upstream chunk lives in memory at a time.**
3. Write the literal `\nendstream\nendobj\n` trailer — one final `ZChannel.write`.

The xref entry's byte size is computed as `header.size + length + trailer.size` so the cross-reference table is correct without ever knowing what came through the stream.

```scala
val attachment: ZStream[Any, Throwable, Byte] =
  ZStream.fromInputStream(...)       // a 100 MB file - never read into a single buffer

val streamingPart: Part[Trailer] = Part.StreamObj(
  index   = Obj.Index(42, 0),
  data    = Prim.dict("Type" -> Prim.Name("EmbeddedFile")),
  length  = 104857600L,
  payload = attachment
)

partStream.via(WritePdf.parts).runFold(0L)(_ + _.size)  // counts bytes; ~64 KiB peak
```

`StreamObjSpec` proves this end-to-end: it encodes a 1 MiB content stream and a 10 MiB content stream by piping a synthetic `ZStream[Byte]` through `WritePdf.parts` directly into a `runFold(0L)(_ + _.size)` byte counter — the total file bytes are never materialised. A separate test then encodes 1 MiB the same way, materialises the bytes, decodes them back, and verifies all 1 048 576 bytes of the deterministic `i & 0xff` pattern come through byte-perfect.

### Unified decode entry point: `PdfStream.decode(log, mode)`

There is a single API with two **modes** (same duplicate-object filtering and final `Meta` in both cases):

```scala
PdfStream.decode(log)                              // DecodeMode.Materialized → Decoded
PdfStream.decode(log, PdfStream.DecodeMode.Streaming) // StreamingDecoded (SAX payloads)
```

`PdfStream.streamingDecode` remains as shorthand for `StreamingDecode.pipeline(Log.noop)`.

**Why two output types at all?** A PDF content stream is not just opaque bytes: the materialized path runs `Content.uncompress`, detects **object streams** (`/Type /ObjStm`) and **xref streams**, and can emit many `Decoded.DataObj` values from one stream. That needs the full decompressed payload (lazy `BitVector`) in memory for that object. The streaming path deliberately forwards **raw** stream bytes in `ContentObjBytes` chunks so peak memory follows the upstream chunk size; it does **not** expand ObjStm / XRef stream payloads into nested objects (you would hash or store the bytes, or switch to materialized decode for that object).

The materialized pipeline materialises each ordinary content stream's payload as a `BitVector` so it can resolve `/Length` and verify the trailing `endstream`. For PDFs with multi-MB attachments / images / fonts this means peak memory is bounded by the largest single object, not by the upstream chunk size — fine for typical text-heavy PDFs, problematic for big binary blobs.

The streaming pipeline is the SAX-style alternative: same top-level coverage (version, comment, xref, startxref, data objects, content objects, accumulated `Meta`), but each content-stream payload is forwarded as a sequence of `ContentObjBytes` chunks instead of being materialised:

```scala
sealed trait StreamingDecoded
object StreamingDecoded {
  case class DataObj(obj: Obj)                                       extends StreamingDecoded
  case class VersionT(v: Version)                                    extends StreamingDecoded
  case class XrefT(x: Xref)                                          extends StreamingDecoded
  case class StartXrefT(s: StartXref)                                extends StreamingDecoded
  case class CommentT(b: ByteVector)                                 extends StreamingDecoded
  // SAX events for one content stream:
  case class ContentObjHeader(obj: Obj, length: Long)                extends StreamingDecoded
  case class ContentObjBytes(bytes: Chunk[Byte])                     extends StreamingDecoded
  case object ContentObjEnd                                           extends StreamingDecoded
  case class Meta(xrefs: List[Xref], trailer: Option[Trailer], version: Option[Version]) extends StreamingDecoded
}
```

Internally the pipeline alternates between two modes:

- **WaitingHeader**: try to decode one of `Version | Xref | StartXref | Comment | IndirectObj.headerOnly` from the carry buffer. The new `IndirectObj.headerOnly` codec stops *just after* the `stream\n` keyword and yields an `IndirectObjHeader(obj, Option[Long])`. On success, emit the matching event (and for stream-bearing objects, a `ContentObjHeader` plus a transition).
- **ForwardingBytes(remaining)**: forward upstream bytes downstream as `ContentObjBytes` chunks, decrementing `remaining`. When it hits zero, parse `\nendstream\nendobj\n` and return to WaitingHeader.

Peak memory = upstream chunk size + the carry buffer for one TopLevel-shaped header. **The actual content stream payload is forwarded chunk-by-chunk; it never lives in a single buffer.**

```scala
import zio.pdf.{PdfStream, StreamingDecoded}
import java.security.MessageDigest

// Hash a 256 KiB embedded blob without ever holding it in memory:
val digest: ZIO[Any, Throwable, Array[Byte]] =
  ZStream
    .fromInputStream(...)                              // big PDF
    .via(PdfStream.streamingDecode)
    .collect { case StreamingDecoded.ContentObjBytes(c) => c }
    .runFold(MessageDigest.getInstance("SHA-256")) { (md, c) =>
      md.update(c.toArray); md
    }
    .map(_.digest())
```

`StreamingDecodeSpec` proves the API works:

- **Header / Bytes\* / End sequence** is emitted exactly once per content object.
- **Byte-perfect concatenation**: all `ContentObjBytes` chunks for one object concatenate to the original payload.
- **1 MiB and 10 MiB streaming payloads** decoded end-to-end without materialisation (verified by `runFold(0L)` byte counters that never collect the chunks).
- **Streaming SHA-256**: piping `ContentObjBytes` straight into `MessageDigest.update` produces the byte-perfect hash without buffering.

When to use which mode:

| Workload | Call |
|---|---|
| Text-heavy PDFs, small content streams, need lazy decompression / ObjStm extraction | `PdfStream.decode(log)` or `decode(log, DecodeMode.Materialized)` |
| Big embedded streams, forward raw bytes to a sink (CDC, S3, hash) without materialising | `PdfStream.decode(log, DecodeMode.Streaming)` or `streamingDecode` |

This finally closes the loop: **both encoder and decoder are now memory-bounded.** `Part.StreamObj` lets the encoder write multi-GB attachments without materialisation; `PdfStream.streamingDecode` lets the decoder read them the same way.

## Content-defined chunking (FastCDC) for storage dedup

`Part.StreamObj` lets the encoder write arbitrarily large payloads without materialising them, but in many PDF authoring/storage workflows multiple documents share the same blob (an embedded font, a logo, a boilerplate attachment). Cutting those payloads at content-defined boundaries makes them **content-addressable**: identical sub-ranges produce identical chunks regardless of where they appear in the stream, so a downstream key/value store can dedup them by chunk hash.

`zio.pdf.cdc.FastCdc.pipeline(cfg): ZPipeline[Any, Throwable, Byte, Chunk[Byte]]` — a memory-bounded ZIO port of FastCDC (Xia et al., USENIX ATC 2016):

```scala
import zio.pdf.cdc.FastCdc

val cdc: ZPipeline[Any, Throwable, Byte, Chunk[Byte]] =
  FastCdc.pipeline()                                  // 4 KiB / 16 KiB / 64 KiB defaults

val attachment: ZStream[Any, Throwable, Byte] = ZStream.fromInputStream(...)
val chunks: ZStream[Any, Throwable, Chunk[Byte]] = attachment.via(cdc)

// Hand each chunk to a dedup store, keyed by hash:
chunks.mapZIO { c =>
  val h = blake3(c)        // or any cheap content hash
  store.putIfAbsent(h, c)
}.runDrain
```

Properties locked down by `FastCdcSpec`:

- **Total bytes preserved**: the concatenation of all CDC chunks equals the input — no bytes added, dropped, or reordered.
- **Size bounds**: every chunk except possibly the last is in `[minSize, maxSize]`; tail can be smaller.
- **Determinism**: chunking the same bytes twice gives identical chunk hashes.
- **Rechunking-invariance**: feeding the same input as 1 chunk vs 7 KiB chunks vs 1 byte at a time produces the **same** CDC chunks. (This is the property that makes dedup actually work — without it, upstream chunk boundaries would change the cut decisions.)
- **Dedup property**: a 1-byte insertion early in the stream perturbs at most ~3 chunks; the rest survive intact.
- **Disjoint-payloads property**: two completely different random payloads share ≤ 2 chunks by accident.
- **Memory-bounded**: 10 MiB random stream chunked end-to-end without materialisation; peak buffer = `maxSize`.
- **Graceful degradation**: highly-structured input (cyclic data) just cuts at `maxSize` instead of looping.

### Throughput

```
Benchmark                   (rechunk)    (size)  Mode  Cnt   Score   Error  Units
FastCdcBench.cdcThroughput      65536  33554432  avgt    5  72.635 ± 4.797  ms/op
```

**~440 MB/s** on the build VM (32 MiB / 72.635 ms, JDK 21). For comparison, the FastCDC paper reports ~590 MB/s in native C; the JVM port is ~75% of native throughput, well past Rabin-Karp's typical 30–50 MB/s.

### Composing CDC with `Part.StreamObj`

`StreamObjCdcSpec` proves the composition end-to-end:

1. Two 256 KiB random payloads that differ only in a 1 KiB middle region are streamed through `FastCdc.pipeline()` independently, never materialised. The resulting CDC chunk-hash sets share **≥ 75%** of chunks; **at most 3 chunks differ**.
2. A 1 MiB streaming PDF content object is encoded via `Part.StreamObj + WritePdf.parts` (memory-bounded) while a separate `FastCdc.pipeline` over the same payload computes the dedup manifest. Neither pipeline ever holds the full payload in memory.

This is what an "embedded-file deduplication" workflow looks like: the encoder stays memory-bounded, the dedup store gets a content-addressable chunk manifest, and a small change to the source payload (e.g. a re-versioned PDF where 99% of the embedded font is identical) only invalidates the chunks that actually contain the change.

### When to use what

- **Want streaming I/O for big payloads, no dedup** → `Part.StreamObj` alone.
- **Want streaming I/O *and* content-addressable storage dedup** → `Part.StreamObj` + `FastCdc.pipeline()` over the payload (in parallel, since the encoder consumes the bytes verbatim).
- **Want fixed-size chunking** → `ZStream#rechunk(N)` is the right tool. Use it for I/O batching, not for dedup; a 1-byte insertion shifts every chunk and dedup ratio drops to zero.
- **Want semantic chunking of *extracted PDF text*** (sentences/paragraphs/headings, for embedding in a vector DB) → that's a different domain (the Rust `text-splitter` crate is a good reference); not part of this port. The natural place to add it is downstream of a future text-extraction layer that produces `ZStream[String]` from a decoded `Page`.

## File-handle lifetime: Scope vs ZIO

`zio-blocks-scope` provides `Resource[A]` + `Scope` + the `$` macro for **compile-time** prevention of resource-escape bugs. The `$[A]` path-dependent type literally cannot leave a `scoped { … }` block — forgetting to close a file is a *type error*, not a runtime bug. This is genuinely interesting and worth wiring into the PDF library.

It does **not** "erase ZIO". The streaming substrate (`ZStream` / `ZPipeline` / `ZChannel`) is not replaced by `Resource`; only the **file-handle ownership boundary** moves. The two APIs sit side-by-side in `zio.pdf.io.PdfIO`:

```scala
import zio.pdf.io.PdfIO
import java.nio.file.Path

// A) Scope-based: synchronous, compile-time leak prevention
val bytes: Chunk[Byte] = PdfIO.scoped.readAll(Path.of("doc.pdf"))
PdfIO.scoped.writeAll(Path.of("out.pdf"), bytes)

// B) ZIO-based: streaming, ZIO-shaped lifetime
val stream: ZStream[Any, Throwable, Byte] = PdfIO.zio.reader(Path.of("big.pdf"))
val sink:   ZSink[Any, Throwable, Byte, Byte, Long] = PdfIO.zio.writer(Path.of("out.pdf"))

stream.via(PdfStream.decode()).runDrain      // file handle closed by ZStream's scope
```

When to use which:

- **`PdfIO.scoped.{readAll, writeAll}`** — small/medium PDFs where you want the strongest possible "did I forget to close this?" guarantee. The `$` macro forbids the file handle from escaping the scoped block, so the InputStream/OutputStream is forced to be consumed inline. Returns pure data (`Chunk[Byte]`, `Long`); side effect is the file I/O.
- **`PdfIO.zio.{reader, writer}`** — large files, async workflows, and anything that wants `ZStream` composition past the function boundary. Standard `ZIO.scoped` lifetime; the stream value flows freely through ZIO-shaped code.

`PdfIOSpec` proves both work:

- Round-trip via either API
- **Resource finalizer runs even when the scoped block throws** (counter test: the close-counter increments to 1 even though `RuntimeException("boom")` propagates out of `Scope.global.scoped`)
- `Resource.flatMap` composes two file handles, finalized in LIFO order
- `PdfIO.zio.reader` streams a 1 MiB file without materialising it
- A real PDF round-trips end-to-end: `PdfIO.zio.reader → PdfStream.decode → Decoded.{DataObj, ContentObj, Meta}`

### Why both APIs

The `$[A]` path-dependent type is the killer feature of `Scope` but it's also its limitation: a value tagged `scope.$[InputStream]` cannot escape the `scoped` block, which is **exactly** what makes a `ZStream.fromInputStream`-style wrapper impossible — that wrapper would have to capture the InputStream in a closure, and the macro forbids capture. So:

- `Scope` is a great fit for synchronous, "open file → process inline → close" workflows.
- For streaming I/O across function boundaries, `ZIO.scoped` + `ZStream.fromInputStream` is the right tool. The cost is that you give up the macro's compile-time leak prevention.

This is a feature, not a bug — `Scope` says, on the tin: "your resource cannot leave this block at compile time". If you want it to leave the block, you've chosen the wrong abstraction. Stay on ZIO's resource scopes for that case.

### Could `Scope` replace ZIO entirely?

For this codebase, no. `ZStream` / `ZPipeline` / `ZChannel` *is* the streaming substrate; the entire decoder + encoder + pipeline layer is built on it. `Scope`/`Resource`/`Wire` solve a different problem (resource lifetime + DI graphs) and would only replace the small set of sites where we currently use `ZIO` directly:

- `Log.live` (`ZIO.logDebug` / `logError`) — could move to a synchronous `Logger` trait wrapped in `Resource`.
- `StatefulPipe.applyEffect`'s `onDone` hook — same, but it's optional.
- `PdfIO.zio.{reader, writer}` — could move to `Scope` if every consumer agreed to call `scoped { ... }` synchronously, but that defeats the streaming use case.

So `zio-blocks-scope` is a **complementary** primitive, not a replacement. We use it where it fits (synchronous file I/O with the strongest possible compile-time guarantees) and stay on `ZIO` for the streaming and async sites where `Scope`'s constraints would be too strict.

## Replacements summary

| legacy | new |
|---|---|
| `cats.effect.IO` | `zio.Task` |
| `fs2.Stream` | `zio.stream.ZStream` |
| `fs2.Pipe` | `zio.stream.ZPipeline` |
| `fs2.Pull` | `zio.stream.ZChannel` |
| `cats.data.NonEmptyList` | `zio.NonEmptyChunk` |
| `cats.data.Validated[NonEmptyList[E], A]` | `zio.prelude.Validation[E, A]` |
| `shapeless.HList` | Scala 3 tuples + `Codec#xmap` |
| `scodec.stream.StreamDecoder` (FS2) | `zio.scodec.stream.StreamDecoder` (ZChannel) |
| (none) | `zio.scodec.schema.ScodecDeriver` for `Schema.derived[A].derive(ScodecDeriver)` |

## License

Copyright 2020-2026 SpringerNature. Apache License 2.0.
