# Generic scans and shared byte ranges

These APIs are introduced in 0.7.0 and ship with the core artifact on JVM and Scala Native.
They contain no PDF or effect-system dependencies. The generic scan executor uses
ZIO Blocks Schema 0.0.56 `Registers`; Native uses the existing public-source
backport. The optional ZIO transport stays in the compatibility artifact.

## Generic composition

`io.github.adrielc.scan.Scan[-I, +O]` transforms arbitrary input values into zero
or more output values. It exposes `type State` and `Scan.Aux[S, I, O]`. Definitions
are reusable; every runner gets fresh state. `map`, `contramap`, `left` and `right`
preserve the state type. `andThen`, `broadcast` and `choose` expose the two state
types as a pair, while their runtime state remains in flat slots.

```scala
import io.github.adrielc.scan.Scan

val totals: Scan.Aux[Long, Int, Long] =
  Scan.foldLong[Int](0L)((sum, value) => sum + value)
val formatted: Scan.Aux[totals.State, String, String] =
  totals.contramap[String](_.length).map(total => s"$total characters")

val compiled = formatted.compile().toOption.get
val run = compiled.open(println)
run.push("abc")
val current: Either[Scan.RunIssue, totals.State] = run.state // Right(3L)
val finalState = run.finish() // emits "3 characters" and returns Right(3L)
```

`andThen` sends each emitted value to the next scan. At EOF, upstream tails flow
through downstream steps before downstream completion. `broadcast` feeds the same
input reference into two independent scans, tagging outputs with `Left`/`Right`.
It does not pair, discard or wait for matching outputs. `choose` routes an `Either`
input to just the selected branch; `Scan.branch(predicate)(yes, no)` constructs
that routing. Both branches finish at source EOF, including a branch which saw
no inputs. A fold in that branch can therefore emit its empty-source result.

## FS2 Scan coverage

The comparison target is the public [FS2 3.14.0 `Scan` source](https://github.com/typelevel/fs2/blob/v3.14.0/core/shared/src/main/scala/fs2/Scan.scala).
Every operation family has a counterpart:

- `apply`, `stateful`, `stateful1`, `stateless`, `lift`: pure constructors,
  with callback emission instead of allocating output chunks.
- `initial`, `transform`, `transformAccumulate`, `onComplete`, `step`: explicit
  state and continuation. Stepping returns typed compile/run failures and sends
  outputs to a callback. Compile once and reuse `Runner` for hot streaming paths.
- `andThen`, `map`, `contramap`, `dimap`: sequential composition and mapping.
- `imapState`: converts state before/after every input, even when conversions
  are not inverses. This adapter materializes logical state each step; ordinary
  unadapted register scans do not.
- `lens`, `first`, `second`: pair each output with its originating input context.
- `semilens`, `semipass`, `left`, `right`: contextual transformation with bypass.
  Bypassed values do not update the child state. These context adapters discard
  EOF outputs, matching FS2; EOF has no originating input.
- `choice`, `choose`: select independent branches. Their EOF tails remain and
  are delivered left then right, before downstream completion.
- `toPipe`, Cats `Functor`, `Contravariant`, `Strong`: optional
  `io.github.adrielc.scan.fs2.ScanFs2` integration, available in `kyo-pdf-fs2`
  and `kyo-pdf-fs2_native0.5_3`. Import `ScanFs2.*` for syntax and
  `ScanFs2.given` for instances. FS2 is not a core dependency.

This is behavioral coverage, not source/binary compatibility with `fs2.Scan`.
Our state type is a member, seeds are by-name per run, and queues are bounded.
Nested context/state adapters have a checked depth limit of 64 and count their
children against the shared node budget. The optional pipe requires Cats Effect
`Sync` to suspend and bracket mutable run-local storage; FS2's immutable scan
also supports pure pipes without that constraint. The default pipe processes at
most `limits.pending` inputs from the current transport chunk before yielding,
retaining fewer than `2 * limits.pending` output references. One input or EOF may
emit at most `limits.pending` outputs. `toPipeElementwise` yields after each input
for precise downstream early termination, trading throughput for less read-ahead.
Both modes close on EOF, error, cancellation or downstream termination. This reference
count is not a bound on arbitrary user payload/state bytes. `toPipeWith` accepts
explicit limits. Queue/batch overflow becomes `ScanException` carrying the typed
issue; source failures and callback defects keep their original exceptions.

Neither pure scan type evaluates effects or terminates its source. Effects,
cancellation and early termination belong to the surrounding stream; terminal
byte-probe early completion belongs to `SharedScan`. The comparison covers the
`fs2.Scan` type, not every similarly named combinator on `fs2.Stream` or `Pull`.

`ScanCompatibilitySpec` and `ScanFs2Spec` exercise these semantics on JVM and
Native, with the latter comparing outputs directly against FS2 3.14.0.

## Observe while forwarding the original input

This extension uses ordinary composition, with no special interpreter node:

```scala
import io.github.adrielc.scan.ObservationSyntax.*
import io.github.adrielc.scan.fs2.ScanFs2.*

// observer: Scan[ByteBlock, Component]
// source: fs2.Stream[cats.effect.IO, ByteBlock]
val mixed = source.through(observer.withInput.toPipe[cats.effect.IO])
// Stream[IO, ByteBlock | Component]
val tagged = source.through(observer.withInputEither.toPipe[cats.effect.IO])
// Stream[IO, Either[ByteBlock, Component]]
```

`withInputEither` is `Scan.identity[I].broadcast(observer)`. `withInput` maps
the tags into `I | O`. Every input is forwarded once, using the same reference,
before its observations; final observations follow at EOF. Choose distinct
nominal input/event types for a union. If their types overlap, use `Either` to
retain provenance. Forwarded inputs must be immutable/owned or explicitly leased
for as long as downstream retains them. This does not extend borrowed lifetimes.
This repository targets Scala 3; the Either encoding does not add Scala 2 builds.

For model-driven use, the host exports registered observations and input/output
handles in its tool schema. The model selects those data definitions; the host
compiles them to scans, routes byte values to the output sink and observations
to metadata, and returns output handles, hashes and observations. That is a
proposed integration contract, not a new deployed observation tool. Scala
callbacks and borrowed capabilities are not serialized into model requests.

To parse once and then run independent checks, compose `parse.andThen(checkA.broadcast(checkB))`.
Using `parse.broadcast(parse)` creates two independent parser states even when
both branches refer to the same definition. User callbacks must respect input
immutability. Domain failures can be represented in the output, such as
`Either[ParseError, Record]`; routing or stopping them is an explicit scan choice.

`stateful(initial)(step, finish)` supports arbitrary state and zero/many emissions
without constructing intermediate output chunks. An emitter is valid only during
its callback. `compile` builds a bounded, iterative instruction graph once;
`open` creates isolated storage for every run. The runner is serial and drains
outputs before `push` returns. `feed` accepts a bounded batch callback and drains
each input immediately, avoiding a result wrapper per element. Its input emitter
expires when the batch callback returns. Source bytes borrowed for a push must remain valid
through that call. Any scan retaining a borrowed value between pushes needs an
explicit lease. Runs close on completion, defects or explicit `close()`.

## State storage and performance

The logical state type stays visible even when the runtime representation differs.
Opaque typed slots prevent accidental mixing of state types inside the executor.
ZIO Blocks registers store Long state without boxing and reference state in a
separate region. `foldLong` and `statefulLong` have primitive callback signatures.
Ordinary generic `stateful[S, ...]` may box primitive S; it is the flexible path.
`run.state` and `finish()` materialize typed state on demand, not on every input.
Mutable state observations refer to the same state object; they are not deep copies.

The inline `Scan.map` constructor embeds the supplied function in a unary stage.
Unary maps, filters and selected routes run in a direct interpreter loop. A bounded
work stack holds pending values and integer targets without per-event envelope
allocations, preserving emitted-value order through branches of unequal depths.
General stateful callbacks still create scoped emitter handles, and arbitrary
outputs can allocate. This is not a claim of a zero-allocation pipeline. Definitions
are checked at Scala compilation; instruction-graph compilation happens explicitly
at runtime and can be reused. There is no new macro compiler or serialized closure
format.

Defaults allow 4,096 definition nodes and 1,024 pending values; checked maxima are
16,384 nodes and 65,536 values. Nodes bound executor structure, not arbitrary user
state or payload sizes. An emitter burst exceeding the pending-value budget fails
with `PendingLimit` and closes the run. Outputs already delivered by earlier
pushes cannot be rolled back. Typed byte-storage limits remain separate below.

## Shared byte input

`io.github.adrielc.bytescan.SharedScan` coordinates incremental byte probes:
framing, signatures, counters, checksums, delimiters or other parsers. Each probe
owns its cursor and state; a session owns the input and retention accounting.
The terminal observation API is called `Probe`, distinct from the streaming `Scan`.

`Probe.bytes(compiledScan, initialResult)(foldOutput)` feeds shared bytes into any
generic scan and incrementally folds its outputs. `Probe.decode` accepts a decoder
callback with state, a borrowed window, EOF, and an emitter; decoded records then
flow through a compiled generic scan. This avoids reparsing input for each check.
The decoder can consume a known-size blob with a Long remaining-byte counter,
emitting records or summaries without collecting the payload.

## Composition and shared work

```scala
import io.github.adrielc.bytescan.SharedScan.*

// A bounded state: two counters, independent of the total source length.
val facts = Probe.foldBytes((0L, 0L)) { case ((sum, zeroes), byte) =>
  (sum + (byte & 255), zeroes + (if byte == 0 then 1L else 0L))
}
val checks = facts.map(_._1).zip(facts.map(_._2)).zip(Probe.count)

val session = Session.open(checks).toOption.get
val bytes = Block.copyOf(Array[Byte](1, 0, 2)).toOption.get
val accepted = session.offer(bytes) // Right(None): EOF is still required
val result = session.finish()      // Right(((3, 1), 3))
session.close()
```

`map` derives a result and `zip` combines results over the same source. Common
stages and mapped results are evaluated once per session, by definition identity.
Two independently constructed scans remain independent. A new session evaluates
each initial-state expression again; allocate mutable seeds inside that expression
and keep scanner callbacks pure. The current implementation executes callbacks
sequentially and sessions/views are confined to one driver.

These are executable Scala definitions, not serialized callbacks. This addition
does not change saved PDF recipe formats. The Kyo PDF scanner, content parser,
object decoder and ZIO input adapters now use shared byte views; see
[PDF parser migration](pdf-parser-migration.md) for boundaries and measurements.

## Incremental scanner contract

Create a scan with `Probe.stateful[E, S, A](initial)(advance)`. Each call receives
its own state, a borrowed `Window` and an EOF flag. A window separates two concepts:

- `window.span`: a `Span[View]` with an owner and checked `[from, until)` coordinates.
  A generic `Span[Owner]` works with any storage owner; slicing changes only the
  coordinates. The byte owner exposes guarded reads, not mutable backing arrays.
- `window.cursor`: `Cursor(retainFrom, position, minimumBytes)`. Consumption can
  advance while retaining an earlier delimiter prefix. The runner validates every
  requested cursor transition.

`Window.start`, `position` and `end` remain convenient accessors. Offsets and
known payload counters use Long. A span does not extend its owner's lifetime;
use a lease when data must survive the callback.

Return one of:

- `Step.Await(state, position, retainFrom, minimumBytes)` to commit consumption
  and keep bytes from `retainFrom`. The next call waits for `minimumBytes` after
  `position`, or EOF. Complete internal phase transitions inside the callback;
  returning the same cursor with an already-satisfied demand is a typed protocol
  failure, preventing a spinning driver.
- `Step.Done(value)` when this scan has its result.
- `Step.Failed(error)` for an expected domain error. The session identifies the
  failing stage and preserves the error type.

The initial call has an empty window, allowing a scanner to declare lookahead
without pulling input. At EOF it must finish, fail, or consume available bytes
before requesting unavailable input. Unfulfilled EOF demand becomes a typed
`UnexpectedEnd` failure.

A known blob length normally belongs in `S` as a remaining-byte counter. Consume
and release each portion incrementally; request full lookahead only when the
algorithm requires all of it. Shared decoded facts can feed multiple mapped
checks. Decompression requires its own bounded output storage/stream.

## Ownership and lifetime

`Block` is a named materialization boundary with a maximum view length of 64 KiB.
`Block.copyOf` checks length before allocation and makes one ownership copy.
Scans then share that block; the coordinator makes no per-scan byte copies.

For an immutable carrier, `Block.borrowImmutable(length, backingBytes)(read)`
performs no copy. Supply the full retained backing-byte cost, including a larger
array held by a slice. The reader must remain valid and immutable while the block
is retained. This contract is the producer's responsibility.

`Block.unsafeBorrowArray` is an explicit mutable-pool boundary. Its producer may
refill the array only when `isRetained` is false after acceptance. Share one block
ownership handle for that array rather than wrapping mutable aliases separately.
The count includes every owning session; an early result from one reader does not
release another reader's bytes.

`Window.read` and `fold` expose no mutable arrays. A borrowed window expires when
its callback returns; escaped views return `AccessIssue.Expired`. Use
`window.pin(from, until)` for an explicit retained `Lease`. Its range survives
subsequent callbacks and early scan completion until the lease/session closes.
`lease.duplicate()` provides an independent lifetime over the same bytes.

Small owned outputs can use `view.detach(from, until)`. It checks the 64 KiB bound
before allocation and can release a much larger pinned backing. All byte reads
hold a transient borrow, so closing a session from a callback cannot recycle
storage in the middle of that read.

The store releases each fully-consumed segment when no scanner or lease needs it.
Older pins do not keep unrelated later segments alive. Repeated references to
one `Block` count its backing bytes once within a session.

## Bounds and backpressure

`Limits.checked` accumulates independent configuration errors. Defaults are:

- 1 MiB of retained backing-byte storage, with an 8 MiB supported maximum.
- 256 segment descriptors, at most 4,096.
- 32 distinct atomic scans, at most 64.
- 128 active pins, at most 1,024; zero disables pins.

`offer` checks capacity before taking ownership. `Failure.Backpressure` leaves
the block unaccepted and the session usable: release no-longer-needed leases,
then retry. A scanner whose required retained window cannot fit must reduce its
lookahead or use a different external storage strategy. Disk spooling and
asynchronous scheduling are not implemented by this kernel.

`Stats` reports accepted bytes, current/peak retained backing bytes and segment
counts, active pins and accepted/released blocks. This is storage accounting,
not total JVM heap or native RSS. Callback state, result objects, definition size,
transport buffering and decoded data require their own bounds. The coordinator
cannot sandbox arbitrary Scala code. Borrowed storage costs are caller-supplied;
use owned ingress when the backing allocation is unknown.

Early completion stops input at transport-chunk granularity. This API coordinates
scans of a source; it is not a parser-combinator API returning an unconsumed tail.

## Capture-checked borrowing (opt-in)

`io.github.adrielc.bytescan.capture.ScopedView` is an experimental facade using
Scala 3.8.4 capture checking. Enable `-experimental` and import
`scala.language.experimental.captureChecking` in callers that use it:

```scala
import scala.language.experimental.captureChecking
import io.github.adrielc.bytescan.capture.ScopedView

// window is a SharedScan.Window supplied by a byte-probe callback.
val first = ScopedView.use(window)(borrow => borrow.read(borrow.from))
// Rejected: a closure would retain the callback's borrowed capability.
// val later = ScopedView.use(window)(borrow => () => borrow.read(borrow.from))
```

Borrowed capabilities may be used inside the callback; ordinary values and owned
`detach` results may leave it. Compiler regressions reject returning the view,
returning a closure, assigning it into an outer variable, and storing it in an
escaping scan's state. Unchecked interop still gets runtime expiration checks.
The underlying byte-owner/session lifecycle is enforced by the same runner on JVM
and Native. Capture checking does not manage memory, allow recycling live buffers,
or make arbitrary Scala callbacks serializable. The regular API does not require
the experimental compiler mode.

Run `sbt 'kyoPdf/checkScanCaptures'` for the positive and negative compiler tests.
The task invokes the project's Scala compiler and always runs uncached. Both CI
and release validation include it.

## ZIO transport

```scala
import io.github.adrielc.bytescan.zio.ZioSharedScan
import zio.ZIO

// bytes: ZStream[R, InputError, Byte]
val run = ZIO.scoped(ZioSharedScan.run(bytes, checks))
```

`run` performs one bounded ownership copy per transport chunk. `runBorrowed`
instead shares immutable ZIO chunks without copying, with an explicit function
reporting each chunk's retained backing-byte cost. A slice's length alone is not
that cost. Both modes reject chunks larger than 64 KiB and bound coordinator
storage independently of total input length. The transport must supply bounded
chunks/prefetch; at most one newly pulled chunk is pending outside the store.

Source errors, scan errors, protocol errors and input-bound failures stay distinct.
Failures/defects invalidate leases and release the session. Source resources and
successful leased outputs follow the enclosing ZIO `Scope`. A completed plan that
needs no bytes never acquires the source; early completion stops further pulls.

Run the portable and transport checks with:

```sh
sbt ';kyoPdf/checkScanCaptures;kyoPdf/testOnly *ScanSpec *ScopedViewSpec;kyoPdfZio/testOnly *ZioSharedScanSpec'
sbt 'kyoPdfNative/testOnly *ScanSpec *ScopedViewSpec'
```

Regressions exercise non-PDF length-prefixed frames, every fragmentation of a
small example, delimiter carry, shared derived checks, distinct reader demands,
backing-byte accounting, independent leases, detach, backpressure/retry, EOF,
interruption, early termination and logical offsets near `Long.MaxValue`.
