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

**Decoder-side caveat**: the decoder still materialises an *individual content stream's* payload as a `BitVector` (because `IndirectObj.streamPayload` needs to resolve `/Length` and confirm the trailing `endstream` keyword on the underlying bytes). For pure decoding workflows on a multi-GB file this means peak memory is bounded by *the largest single object*, not by the total file size. Streaming the payload of an individual object back out of the decoder would require splitting `IndirectObj.streamPayload` into a header decoder + a payload `ZPipeline`; that's a follow-up.

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
