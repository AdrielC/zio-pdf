/*
 * Content-defined chunking via FastCDC (Xia et al., USENIX ATC 2016).
 *
 * Why this is here: `Part.StreamObj` already lets the encoder write
 * arbitrarily large payloads without materialising them, but in many
 * PDF authoring/storage workflows multiple documents share the same
 * embedded blob (an embedded font, a logo, a boilerplate PDF
 * attachment). Cutting those payloads at content-defined boundaries
 * makes them *content-addressable*: identical sub-ranges produce
 * identical chunks regardless of where they appear in the stream,
 * so a downstream key/value store can dedup them by chunk hash.
 *
 * FastCDC vs the alternatives:
 *
 *   - Fixed-size chunking (every N bytes): trivial but a 1-byte
 *     insertion at offset 0 shifts every subsequent chunk - useless
 *     for dedup.
 *   - Rabin-Karp CDC (the "classic" rolling-hash CDC): O(window)
 *     per byte; ~30-50 MB/s in pure JVM.
 *   - Gear-hash CDC / FastCDC: O(1) per byte; ~300-500 MB/s.
 *     Plus FastCDC adds "normalised chunking" (bias the mask
 *     bit-count by current chunk size) which keeps chunk-size
 *     variance much lower than plain gear hashing.
 *
 * The implementation here is a faithful port of the FastCDC paper:
 *
 *   - 64-byte gear-hash table (256 random Long values, deterministic
 *     so the chunker is reproducible across runs / nodes).
 *   - Hard min and hard max chunk sizes; below `minSize` no cut is
 *     made; above `maxSize` an unconditional cut is made.
 *   - Two masks: a "small" mask (more bits set => harder cut) used
 *     while the chunk is still smaller than the average, and a
 *     "large" mask (fewer bits set => easier cut) used after, so
 *     the average chunk size is biased towards the configured target.
 *
 * The pipeline is memory-bounded: at most one in-flight chunk's
 * worth of bytes (= `maxSize`) is buffered.
 */

package zio.pdf.cdc

import zio.{Cause, Chunk}
import zio.stream.{ZChannel, ZPipeline}

object FastCdc {

  /** Tuning parameters. Defaults match the FastCDC paper's
    * recommended values for the 8 KiB / 16 KiB / 64 KiB regime,
    * which is what most dedup systems (restic, borg, casync) use. */
  final case class Config(
    minSize: Int = 4 * 1024,    // hard minimum chunk size
    avgSize: Int = 16 * 1024,   // target average chunk size
    maxSize: Int = 64 * 1024    // hard maximum chunk size
  ) {
    require(minSize > 0, "minSize must be positive")
    require(avgSize >= minSize, "avgSize must be >= minSize")
    require(maxSize >= avgSize, "maxSize must be >= avgSize")
    require(java.lang.Long.bitCount(avgSize.toLong) == 1, "avgSize must be a power of two")

    /** log2(avgSize). Used to derive the small/large masks. */
    val avgBits: Int = java.lang.Integer.numberOfTrailingZeros(avgSize)

    /** The "small" mask used while the chunk is still smaller than
      * the average. More bits => boundary-condition is rarer => we
      * push past short cut candidates. */
    val maskSmall: Long = (1L << (avgBits + 2)) - 1L

    /** The "large" mask used after the chunk has reached its
      * average. Fewer bits => boundary-condition is commoner =>
      * we cut soon, capping chunk sizes near the average. */
    val maskLarge: Long = (1L << (avgBits - 2)) - 1L
  }

  /** Default config: 4 KiB min, 16 KiB avg, 64 KiB max. */
  val defaultConfig: Config = Config()

  /**
   * Pre-computed gear-hash table: 256 random Longs, one per byte
   * value. The exact values don't matter as long as they're well-
   * distributed; we hard-code them with a deterministic PRNG so
   * the chunker is reproducible across runs and machines.
   */
  private val Gear: Array[Long] = {
    val rng = new java.util.Random(0x9E3779B97F4A7C15L)
    Array.fill(256)(rng.nextLong())
  }

  /**
   * Find the next FastCDC cut point in `buffer`, scanning forward
   * from offset 0. Returns the cut offset (1-indexed: a return of
   * `n` means "cut after byte n-1"). Always returns a value in
   * `[min(buffer.length, minSize)+1, min(buffer.length, maxSize)]`.
   *
   * If the buffer is shorter than `minSize`, returns `buffer.length`.
   */
  private[cdc] def cutOffset(buffer: Array[Byte], cfg: Config): Int = {
    val n = buffer.length
    if (n <= cfg.minSize) return n

    val maxScan = math.min(n, cfg.maxSize)
    val avgScan = math.min(cfg.avgSize, maxScan)
    var i       = cfg.minSize
    var hash    = 0L

    // Phase 1: between minSize and avgSize, use the strict mask.
    // We start the gear-hash from minSize (the first `minSize - 1`
    // bytes can't produce a cut anyway, by definition of minSize).
    while (i < avgScan) {
      hash = (hash << 1) + Gear(buffer(i) & 0xff)
      if ((hash & cfg.maskSmall) == 0L) return i + 1
      i += 1
    }

    // Phase 2: between avgSize and maxSize, use the lenient mask.
    while (i < maxScan) {
      hash = (hash << 1) + Gear(buffer(i) & 0xff)
      if ((hash & cfg.maskLarge) == 0L) return i + 1
      i += 1
    }

    // Phase 3: hard cut at maxSize (or buffer end, whichever is first).
    maxScan
  }

  /**
   * Memory-bounded `ZPipeline` from raw bytes to content-defined
   * chunks. Each output `Chunk[Byte]` is a CDC chunk; the
   * concatenation of the outputs equals the concatenation of the
   * inputs (no bytes added, dropped, or reordered). Chunk sizes
   * are constrained to `[minSize, maxSize]`, with average ≈ `avgSize`.
   *
   * Buffer footprint: at most `maxSize` bytes are held at any time
   * (one in-flight chunk).
   */
  def pipeline(cfg: Config = defaultConfig): ZPipeline[Any, Throwable, Byte, Chunk[Byte]] =
    ZPipeline.fromChannel(channel(cfg))

  private def channel(
    cfg: Config
  ): ZChannel[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[Chunk[Byte]], Unit] = {

    /** Drain the buffer into as many CDC chunks as possible.
      *
      * To stay rechunking-invariant we must only ever ask for a
      * cut when we are *guaranteed* to have looked at the same set
      * of input bytes that a single-shot pass would have looked
      * at. That means: only cut when the buffer is at least
      * `maxSize` long (so `cutOffset`'s phase-1 / phase-2 / phase-3
      * search window is fully populated) - or when `flushTail` is
      * true and we're at end-of-stream. */
    def drain(
      buffer: Array[Byte],
      flushTail: Boolean
    ): (Chunk[Chunk[Byte]], Array[Byte]) = {
      val out = Chunk.newBuilder[Chunk[Byte]]
      var buf = buffer
      while (buf.length >= cfg.maxSize || (flushTail && buf.nonEmpty)) {
        // Only feed `cutOffset` as many bytes as it might actually
        // consume, so the search window is well-defined regardless
        // of how much extra we have buffered.
        val window =
          if (buf.length >= cfg.maxSize) java.util.Arrays.copyOfRange(buf, 0, cfg.maxSize)
          else buf
        val cut    = cutOffset(window, cfg)
        val chunk  = java.util.Arrays.copyOfRange(buf, 0, cut)
        val rest   = java.util.Arrays.copyOfRange(buf, cut, buf.length)
        out += Chunk.fromArray(chunk)
        buf = rest
      }
      (out.result(), buf)
    }

    def loop(
      buffer: Array[Byte]
    ): ZChannel[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[Chunk[Byte]], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[Chunk[Byte]], Unit](
        (chunk: Chunk[Byte]) => {
          if (chunk.isEmpty) loop(buffer)
          else {
            // Append the new chunk to the carry buffer.
            val incoming = chunk.toArray
            val merged   = new Array[Byte](buffer.length + incoming.length)
            System.arraycopy(buffer, 0, merged, 0, buffer.length)
            System.arraycopy(incoming, 0, merged, buffer.length, incoming.length)

            val (emit, rest) = drain(merged, flushTail = false)
            if (emit.isEmpty) loop(rest)
            else ZChannel.write(emit) *> loop(rest)
          }
        },
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any) => {
          // End of upstream: flush whatever is left, even if it's
          // shorter than minSize.
          val (emit, _) = drain(buffer, flushTail = true)
          if (emit.isEmpty) ZChannel.unit
          else ZChannel.write(emit) *> ZChannel.unit
        }
      )

    loop(new Array[Byte](0))
  }
}
