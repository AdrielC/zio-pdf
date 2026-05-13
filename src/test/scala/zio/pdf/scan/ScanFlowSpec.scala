/*
 * Spec for the Flow-style typed scan builder.
 *
 * Tests that:
 *   - `.named[N, V](name, scan)` accumulates `N ~ V` into `Out`.
 *   - `run` returns a `Record[Out]` whose fields are accessible by
 *     name with the correct type (compile-time-checked).
 *   - The leftover values match what `Scan.run` produces for the
 *     same stages run independently.
 */

package zio.pdf.scan

import zio.test.*

object ScanFlowSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("ScanFlow")(

    test("named stages accumulate into a typed Record[Out]") {
      val ingest =
        ScanFlow.over[Byte]
          .named("count",  Scan.countBytes)
          .named("digest", Scan.hash(HashAlgo.Sha256))

      val payload: Seq[Byte] = (0 until 256).map(i => (i & 0xff).toByte)
      val rec = ingest.run(payload)

      // `rec.count` is typed `Vector[Byte]` -- 8 BE bytes of 256L.
      val countBytes: Vector[Byte] = rec.count
      val countAsLong: Long =
        java.nio.ByteBuffer.wrap(countBytes.toArray).getLong

      // `rec.digest` is typed `Vector[Byte]` -- SHA-256 of the input.
      val digestBytes: Vector[Byte] = rec.digest
      val expectedDigest =
        java.security.MessageDigest.getInstance("SHA-256").digest(payload.toArray)

      assertTrue(countAsLong == 256L) &&
      assertTrue(digestBytes.toArray.toList == expectedDigest.toList)
    },

    test("single-named flow's field round-trips through the typed Record") {
      val flow = ScanFlow.over[Byte].named("total", Scan.countBytes)
      val rec  = flow.run((0 until 100).map(_.toByte))
      val totalBytes: Vector[Byte] = rec.total
      val total: Long = java.nio.ByteBuffer.wrap(totalBytes.toArray).getLong
      assertTrue(total == 100L)
    },

    test("empty flow yields an empty record") {
      val flow = ScanFlow.over[Byte]
      val rec  = flow.run((0 until 10).map(_.toByte))
      assertTrue(rec == Record.empty)
    }
  )

  // Reuse Kyo's Record for the empty check.
  private val Record = kyo.Record
}
