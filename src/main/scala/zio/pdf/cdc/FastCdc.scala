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
    require(avgSize >= 4, "avgSize must be at least 4 bytes")
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

    final case class State(buffer: Array[Byte], filled: Int, hash: Long)

    val maxInputStep = 64 * 1024

    def processSlice(state: State, input: Chunk[Byte], from: Int, until: Int): (Chunk[Chunk[Byte]], State) = {
      val emitted = Chunk.newBuilder[Chunk[Byte]]
      var filled  = state.filled
      var hash    = state.hash
      var index   = from

      while index < until do {
        val byte = input(index)
        state.buffer(filled) = byte
        filled += 1

        val hardCut = filled == cfg.maxSize
        val hashCut =
          if filled <= cfg.minSize || hardCut then false
          else {
            val byteIndex = filled - 1
            hash = (hash << 1) + Gear(byte & 0xff)
            val mask = if byteIndex < cfg.avgSize then cfg.maskSmall else cfg.maskLarge
            (hash & mask) == 0L
          }

        if hardCut || hashCut then {
          emitted += Chunk.fromArray(java.util.Arrays.copyOf(state.buffer, filled))
          filled = 0
          hash = 0L
        }
        index += 1
      }

      (emitted.result(), State(state.buffer, filled, hash))
    }

    def consumeInput(
      state: State,
      input: Chunk[Byte],
      offset: Int
    ): ZChannel[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[Chunk[Byte]], Unit] =
      if offset >= input.length then loop(state)
      else {
        val nextOffset      = math.min(input.length, offset + maxInputStep)
        val (emitted, next) = processSlice(state, input, offset, nextOffset)
        val continue        = consumeInput(next, input, nextOffset)
        if emitted.isEmpty then continue else ZChannel.write(emitted) *> continue
      }

    def loop(
      state: State
    ): ZChannel[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[Chunk[Byte]], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[Chunk[Byte]], Unit](
        (chunk: Chunk[Byte]) => if chunk.isEmpty then loop(state) else consumeInput(state, chunk, 0),
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any) => {
          if state.filled == 0 then ZChannel.unit
          else
            ZChannel.write(
              Chunk.single(Chunk.fromArray(java.util.Arrays.copyOf(state.buffer, state.filled)))
            ) *> ZChannel.unit
        }
      )

    loop(State(new Array[Byte](cfg.maxSize), 0, 0L))
  }
}
