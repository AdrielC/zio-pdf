# Scan performance against FS2 3.14.0

The default bounded batched pipe is faster on the recorded workload on both JVM
and Native. This does not make every generic scan faster than FS2: the small JVM
kernels remain slower, while allocating fewer bytes. Native mapping/choice wins
and prefix/context losses are included below. Use the transport that fits the
required read-ahead; performance is not a substitute for ownership safety.

Measured source: `9355a296e9e9c168aee10ef3a6e175676636bece`.
Apple M3 Pro arm64 macOS, Scala 3.8.4, Temurin 24.0.2 (3 GiB heap, four active
processors), Scala Native 0.5.12 debug/immix/no LTO. These are local measurements
on a shared host, not an isolated lab or tuned Native release build.

Each run processes 32,768 prebuilt inputs. Definitions/plans are built outside
timing; fresh run state, completion and result checksumming are included. Both
sides produce equal checksums before timing. Fifteen warmup pairs precede 21
alternating measurement pairs; the table reports median milliseconds. Kernels
use per-input FS2 transforms versus the compiled Scan batch driver. Pipe cases
use actual FS2 streams, a chunked source, and Cats Effect IO on both sides.

| Case | JVM FS2 → ours (ms) | JVM FS2 / ours | Native FS2 → ours (ms) | Native FS2 / ours |
|---|---:|---:|---:|---:|
| running-sum-kernel | 0.508 → 0.717 | 0.71× | 11.646 → 12.820 | 0.91× |
| mapped-sum-kernel | 1.409 → 2.360 | 0.60× | 21.130 → 12.798 | 1.65× |
| contextual-first-kernel | 0.731 → 2.260 | 0.32× | 21.731 → 27.862 | 0.78× |
| choose-kernel | 0.877 → 2.018 | 0.43× | 28.002 → 22.798 | 1.23× |
| chunked-source-pipe | 5.995 → 4.043 | 1.48× | 33.618 → 18.520 | 1.82× |
| elementwise-source-pipe | 5.854 → 13.357 | 0.44× | 32.265 → 212.141 | 0.15× |

A factor above 1 favors our implementation; below 1 favors FS2.
The JVM kernels allocate 21–78% fewer current-thread bytes in this run despite
being slower. Allocation is unavailable for Native. IO pipe allocation is also
left unavailable because execution can move to another thread. These are not
peak-memory measurements.

The default pipe processes at most `limits.pending` inputs from the current
transport chunk before yielding. Its output buffer contains fewer than twice
that many references, with at most `limits.pending` emissions per input/EOF.
`toPipeElementwise` yields after each input and retains precise scan-step early
termination within an already received transport chunk. FS2's reference pipe
processes the source chunk, so the elementwise comparison has different
read-ahead semantics and should not be treated as an equal-latency ranking.

The raw data also preserves the earlier implementation: per-stage queue work and
one effect per input made the original pipe much slower. Direct unary execution
and bounded batching improve throughput; the elementwise result shows the cost
of retaining fine-grained yielding. Bounds, cancellation, emitted-value ordering
through unequal branches, and error/EOF behavior have separate JVM/Native tests.

[Raw measurements](benchmarks/scan-fs2-2026-11-05.json).
[PDF parser comparison](pdf-parser-migration.md) measures the actual PDF path
against public v0.6.0, rather than inferring PDF performance from these toy scans.

Reproduce:

```sh
bash scripts/benchmark-scans.sh jvm
bash scripts/benchmark-scans.sh native
```

No timing thresholds run in CI; noisy host timing must not turn a correctness
check into a flaky speed gate.
