# Scala Native (`native/`)

Phase-1 scaffold for a native zio-pdf artifact. The shared PDF decoder still
lives in `src/main/scala`; this module adds platform I/O and build wiring.

## What works today

- `sbt native/compile` — platform layer (`PdfSource`, `NativeFileBackend`, `PdfMime`)
- `sbt native/test` — ZIO Test smoke specs for chunk sources and MIME helpers
- **POSIX file reads** via `PdfSource.fromPath(..., backend = NativeFileBackend.posix)`

## io_uring (phase 2)

`NativeFileBackend.ioUring` exists as a seam. It currently delegates to the POSIX
`read(2)` loop so callers can opt into the backend name without API churn.

To wire real io_uring:

1. Add minimal `@extern` bindings to [liburing](https://github.com/axboe/liburing)
   (`io_uring_queue_init`, `io_uring_prep_read`, `io_uring_submit`, `io_uring_wait_cqe`).
2. Link the native artifact with `-luring` (Linux 5.1+).
3. Batch reads in `IoUringFileBackend.readStream` and integrate with ZIO blocking
   or a dedicated Native runtime executor.
4. Keep `NativeFileBackend.posix` as the non-Linux fallback.

io_uring is optional for `PdfEngine.decode(bytes)` — it matters for high-throughput
file ingest replacing JVM `FileChannel` / mmap hyperdrive paths.

## Shared sources (blocked)

# Flip `NativeIncludeSharedSources` in `build.sbt` to `true` once **zio-blocks 0.0.51+**

Maven Native artifacts today stop at **0.0.14** with an incompatible API:

| JVM / JS (0.0.51) | Native (0.0.14) |
|---|---|
| `zio.blocks.streams.io.Reader` | missing (only `ZSource`/`ZSink` stubs) |
| `zio.blocks.chunk.ChunkMap` | missing |
| `zio.blocks.typeid.TypeId` | missing |
| `zio-blocks-ringbuffer` | not published |

Until that lands, the native project compiles only `native/src/**`. When unblocked,
`nativeSharedSources` reuses the JVM tree minus mmap/pipe/arrow/tacit exclusions
(and supplies Native shims for `PdfEngine`, `BlocksLift`, `PdfMime`).

## Commands

```bash
sbt native/compile
sbt native/test
# full decode on Native (after shared sources are enabled):
# sbt 'set NativeIncludeSharedSources := true' native/test  # after flipping in build.sbt
```
