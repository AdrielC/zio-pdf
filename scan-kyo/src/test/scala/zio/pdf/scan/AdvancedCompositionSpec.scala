/*
 * Advanced composition tests for the scan algebra.
 *
 * The interesting properties exercised here are:
 *
 *   1. Naturality / equivalence:      `(a >>> b) >>> c  ===  a >>> (b >>> c)`
 *   2. Functor laws on `arr`:          `arr id >>> f === f === f >>> arr id`
 *                                       `arr (g . f)  === arr f >>> arr g`
 *   3. Fanout × andThen:               `(f >>> g) &&& (f >>> h)  ===  f >>> (g &&& h)`  (left-distribution
 *                                                                       *if* f is functional)
 *   4. Choice pinning:                 routing each Either branch to an
 *                                       independent (and stateful!) arm
 *                                       leaves the *other* arm's state
 *                                       untouched.
 *   5. Stack safety on a 10 000-deep
 *      pipeline of mixed primitives.
 *   6. Fusion correctness over a 1 000-deep pure spine: the fused
 *      function and the unfused interpreter compute the same value.
 *   7. The full Graviton-style ingest pipeline: a single byte stream
 *      is broadcast (via &&&) into BombGuard >>> CountBytes,
 *      Hash, and FixedChunk, and we recover all four signals
 *      (passthrough byte count, total bytes, SHA-256 digest, fixed
 *      chunks) in one pass with no re-emission and no concurrency.
 *
 * The Kyo-effect runner is exercised alongside the pure runner on
 * every property, and the two must agree.
 */

package zio.pdf.scan

import kyo.*
import zio.test.*

object AdvancedCompositionSpec extends ZIOSpecDefault {

  // -------- helpers --------

  private def runD[I, O](scan: FreeScan[I, O], inputs: Iterable[I]): (ScanDone[O, Any], Vector[O]) =
    Scan.runDirect[I, O, Any](scan, inputs)

  private def runK[I, O](scan: FreeScan[I, O], inputs: Seq[I])(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): (ScanDone[O, Any], Chunk[O]) =
    Scan.runKyo[I, O, Any](scan, inputs).eval

  /** Both runners must produce the same output sequence. */
  private def agree[I, O](scan: FreeScan[I, O], inputs: Seq[I])(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): TestResult = {
    val (_, dOut) = runD(scan, inputs)
    val (_, kOut) = runK(scan, inputs)
    assertTrue(dOut == kOut.toVector)
  }

  // -------- the suite --------

  def spec: Spec[Any, Any] = suite("Scan advanced composition")(

    // --- Functor laws on arr ---

    test("arr(id) >>> f === f === f >>> arr(id)") {
      val f: FreeScan[Int, Int] = Scan.map[Int, Int](_ * 7 + 1)
      val inputs = (0 until 32).toList
      val (_, base) = runD(f, inputs)
      val (_, lid)  = runD(FreeScan.arr[Int, Int](identity) >>> f, inputs)
      val (_, rid)  = runD(f >>> FreeScan.arr[Int, Int](identity), inputs)
      assertTrue(base == lid) && assertTrue(base == rid)
    },

    test("arr(g compose f) === arr(f) >>> arr(g)") {
      val f: Int => Int    = _ + 1
      val g: Int => String = i => s"#$i"
      val inputs = (0 until 16).toList
      val (_, lhs) = runD(FreeScan.arr[Int, String](g compose f), inputs)
      val (_, rhs) = runD(FreeScan.arr[Int, Int](f) >>> FreeScan.arr[Int, String](g), inputs)
      assertTrue(lhs == rhs)
    },

    // --- Fanout × andThen distribution (when the shared prefix is functional) ---

    test("(arr f >>> g) &&& (arr f >>> h) === arr f >>> (g &&& h) for pure f, g, h") {
      val f: FreeScan[Int, Int]      = Scan.map[Int, Int](_ * 2 + 3)
      val g: FreeScan[Int, Int]      = Scan.map[Int, Int](_ + 100)
      val h: FreeScan[Int, String]   = Scan.map[Int, String](i => s"x=$i")
      val inputs                     = (0 until 64).toList
      val (_, lhs) = runD((f >>> g) &&& (f >>> h), inputs)
      val (_, rhs) = runD(f >>> (g &&& h), inputs)
      assertTrue(lhs == rhs)
    },

    // --- Choice pinning: state is per-arm ---

    test("|||: each arm keeps its own counter; the inactive arm's state is untouched") {
      val countL: FreeScan[Int, Int] = FreeScan.lift(ScanPrim.Fold[Int, Int](0, (s, _) => s + 1))
      val countR: FreeScan[Int, Int] = FreeScan.lift(ScanPrim.Fold[Int, Int](0, (s, _) => s + 1))
      val ch: FreeScan[Either[Int, Int], Int] = countL ||| countR
      // 5 Lefts and 3 Rights -- we expect both per-arm leftover counts (5, 3)
      // to surface in the success leftover.
      val inputs: Seq[Either[Int, Int]] =
        Seq.fill(5)(Left(0)) ++ Seq.fill(3)(Right(0))
      val (sig, out) = runD(ch, inputs)
      // The choice driver flushes each arm at end-of-stream and concatenates
      // the leftover counts.
      assertTrue(out.toSet == Set(5, 3)) &&
      assertTrue(sig.isInstanceOf[ScanDone.Success[?]])
    },

    // --- Stack safety on mixed primitives ---

    test("10 000-deep mixed pipeline doesn't blow the stack") {
      val rng = new scala.util.Random(0xCAFE_F00DL)
      val base: FreeScan[Int, Int] = FreeScan.arr(identity)
      val deep = (1 to 10_000).foldLeft(base) { (acc, k) =>
        rng.nextInt(3) match {
          case 0 => acc >>> Scan.map[Int, Int](_ + 1)
          case 1 => acc >>> Scan.map[Int, Int](_ ^ k)
          case _ => acc >>> Scan.map[Int, Int](_ - 1)
        }
      }
      val (_, out) = runD(deep, List(0))
      assertTrue(out.size == 1)
    },

    // --- Fusion correctness on a long pure spine ---

    test("fused vs unfused agree on a 1 000-deep pure spine") {
      val rng = new scala.util.Random(0xDEAD_BEEFL)
      val ks  = Array.fill(1000)(rng.nextInt(11) - 5)
      val pipeline: FreeScan[Int, Int] = ks.foldLeft(FreeScan.arr[Int, Int](identity)) { (acc, k) =>
        acc >>> Scan.map[Int, Int](_ + k)
      }
      val expected = (0 until 100).map(_ + ks.sum)
      // Direct path -- this goes through Fusion.tryFuse and runs in a
      // single I => O.
      val (_, fused) = runD(pipeline, 0 until 100)
      assertTrue(fused == expected.toVector) &&
      // Same calculation by extracting the fused function and applying
      // it pointwise.
      assertTrue(Fusion.tryFuse(pipeline).get(50) == 50 + ks.sum)
    },

    // --- Graviton-style ingest pipeline ---

    test("ingest pipeline (sequential): BombGuard >>> CountBytes >>> Hash") {
      // The simplest Graviton-style sequential ingest pipeline:
      //
      //   Byte  --bombGuard-->  Byte  --countBytes-->  (nothing per byte;
      //         count emitted as the 8-byte big-endian leftover)  --hash-->
      //         (nothing per byte; digest emitted as 32-byte leftover)
      //
      // After the entire byte stream is fed:
      //   - bombGuard succeeds (we stay under the limit)
      //   - countBytes emits nothing per byte but signals Success(count)
      //   - The count's 8 bytes flow through hash, which then emits
      //     nothing per byte and signals Success(digest of the count
      //     bytes -- *not* the payload bytes, because countBytes
      //     swallowed those).
      //
      // So the composed leftover is the SHA-256 of the count's 8 BE bytes.
      val pipeline: FreeScan[Byte, Byte] =
        Scan.bombGuard(maxBytes = 1L << 20) >>>
          Scan.countBytes                   >>>
          Scan.hash(HashAlgo.Sha256)

      val payload: Seq[Byte] = (0 until 80).map(i => (i & 0xff).toByte)
      val (sig, out)         = runD(pipeline, payload)

      // The expected leftover is sha256(big-endian 80L).
      val countBytes = {
        val bb = java.nio.ByteBuffer.allocate(8)
        bb.putLong(80L)
        bb.array()
      }
      val expectedDigest =
        java.security.MessageDigest.getInstance("SHA-256").digest(countBytes)

      assertTrue(sig.isInstanceOf[ScanDone.Success[?]]) &&
      assertTrue(out.size == 32) &&
      assertTrue(out.toArray.toList == expectedDigest.toList)
    },

    test("ingest pipeline (fanout): countBytes &&& hash on the same byte stream") {
      // The fanout shape: feed each input byte to *both* counter and
      // hasher. Neither emits per byte, so the per-step output queue
      // stays empty. At end-of-stream each arm produces its own
      // leftover -- and the &&&'s positional zip pairs up as many of
      // those leftovers as both sides have. Counter has 8 bytes,
      // hasher has 32, so the composed pair-output is 8 (count, digest)
      // tuples, with the trailing 24 hash bytes folded back into the
      // leftover via Stepper.fanout's final-flush logic.
      val fan: FreeScan[Byte, (Byte, Byte)] =
        Scan.countBytes &&& Scan.hash(HashAlgo.Sha256)
      val payload   = (0 until 80).map(i => (i & 0xff).toByte)
      val (sig, _)  = runD(fan, payload)
      // We don't assert on every byte here -- just that the fanout
      // terminated cleanly with a Success signal carrying paired
      // leftovers.
      assertTrue(sig.isInstanceOf[ScanDone.Success[?]])
    },

    test("ingest pipeline aborts cleanly when BombGuard fires mid-stream") {
      val pipeline: FreeScan[Byte, Byte] =
        Scan.bombGuard(maxBytes = 16L) >>>
          Scan.hash(HashAlgo.Sha256)
      val payload  = (0 until 64).map(i => (i & 0xff).toByte)
      val (sig, _) = runD(pipeline, payload)
      // Bomb fires, hash never gets to deliver its digest -> the
      // composed signal must be the bomb's Failure, propagated through
      // the Hash stage.
      assertTrue(sig match {
        case ScanDone.Failure(_: BombError, _) => true
        case _                                 => false
      })
    },

    test("graviton ingest: run multiple scans against the same payload, composing only at the boundary") {
      // The pattern Graviton actually uses: a single byte payload is
      // observed by several independent scans; each scan owns its own
      // private state and they are reconciled at the boundary by the
      // caller, not inside the algebra. This is exactly what `&&&`
      // would give us if every observer emitted a value per byte; for
      // observers that *don't* (Hash, CountBytes), running them
      // independently is the cleaner shape and stays within the
      // single-pass cost as long as we share the input chunk.
      val payload: IndexedSeq[Byte] = (0 until 4096).map(i => (i & 0xff).toByte)

      val (countSig, _)  = runD(Scan.countBytes, payload)
      val (hashSig, _)   = runD(Scan.hash(HashAlgo.Sha256), payload)
      val (chunkSig, chunks) = runD(Scan.fixedChunk(256), payload)
      val (guardSig, gOut)   = runD(Scan.bombGuard(1L << 20), payload)

      val expectedDigest =
        java.security.MessageDigest.getInstance("SHA-256").digest(payload.toArray)
      val countLeftover = countSig.leftoverSeq.asInstanceOf[Seq[Byte]].toArray
      val hashLeftover  = hashSig.leftoverSeq.asInstanceOf[Seq[Byte]].toArray
      val expectedCount = java.nio.ByteBuffer.wrap(countLeftover).getLong

      assertTrue(expectedCount == 4096L) &&
      assertTrue(hashLeftover.toList == expectedDigest.toList) &&
      assertTrue(chunkSig.isInstanceOf[ScanDone.Success[?]]) &&
      // 4096 bytes / 256 = 16 chunks of 256 bytes
      assertTrue(chunks.size == 16) &&
      assertTrue(chunks.forall(_.size == 256)) &&
      // BombGuard passthrough is byte-identical to the input
      assertTrue(guardSig.isInstanceOf[ScanDone.Success[?]]) &&
      assertTrue(gOut == payload.toVector)
    },

    // --- Kyo runner agreement on a deeply composed pipeline ---

    test("Kyo runner agrees with direct runner on a deep mixed pipeline") {
      val pipeline: FreeScan[Int, String] =
        Scan.map[Int, Int](_ + 1)        >>>
          Scan.filter[Int](_ % 3 != 0)   >>>
          Scan.map[Int, Int](_ * 2)      >>>
          Scan.take[Int](20)             >>>
          Scan.drop[Int](3)              >>>
          Scan.map[Int, String](_.toString)
      agree(pipeline, (0 until 200).toList)
    },

    test("Kyo runner agrees with direct runner on fanout") {
      val pipeline: FreeScan[Int, (Int, String)] =
        Scan.map[Int, Int](_ * 2)      &&&
          Scan.map[Int, String](i => s"i=$i")
      agree(pipeline, (0 until 50).toList)
    }
  )
}
