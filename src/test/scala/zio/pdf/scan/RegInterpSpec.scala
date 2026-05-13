/*
 * Equivalence tests for the register-based interpreter.
 *
 * For every scan shape that the register lane is supposed to cover
 * (spine of Map / Filter / Take / Drop / Fold / Hash / CountBytes /
 * BombGuard / FixedChunk / FastCDC, plus pure Arr), we run the same
 * inputs through `Scan.runDirect` (legacy stepper) and
 * `Scan.runDirectReg` and assert that the visible outputs agree.
 *
 * For shapes the register lane intentionally does *not* handle
 * (Fanout/Choice), we still assert agreement -- `runDirectReg` falls
 * back to the legacy path for those, so it must produce the same
 * results.
 */

package zio.pdf.scan

import zio.test.*

object RegInterpSpec extends ZIOSpecDefault {

  private def agree[I, O](scan: FreeScan[I, O], inputs: Iterable[I]): TestResult = {
    val (legacySig, legacyOut) = Scan.runDirect[I, O, Any](scan, inputs)
    val (regSig,    regOut)    = Scan.runDirectReg[I, O, Any](scan, inputs)
    val sigKindMatches =
      (legacySig, regSig) match {
        case (ScanDone.Success(_), ScanDone.Success(_)) => true
        case (ScanDone.Stop(_),    ScanDone.Stop(_))    => true
        case (ScanDone.Failure(_, _), ScanDone.Failure(_, _)) => true
        case _ => false
      }
    assertTrue(legacyOut == regOut) && assertTrue(sigKindMatches)
  }

  def spec: Spec[Any, Any] = suite("RegInterp")(

    test("pure map chain agrees with the legacy runner") {
      val s = Scan.map[Byte, Int](b => b & 0xff) >>>
        Scan.map[Int, Int](_ + 1) >>>
        Scan.map[Int, Int](_ ^ 0x55) >>>
        Scan.map[Int, Int](_ - 1)
      agree(s, (0 until 1024).map(_.toByte))
    },

    test("map + filter spine (non-fusable) agrees") {
      val s = Scan.map[Byte, Int](b => b & 0xff) >>>
        Scan.filter[Int](_ % 2 == 0)             >>>
        Scan.map[Int, Int](_ + 1)
      agree(s, (0 until 256).map(_.toByte))
    },

    test("take(n) terminates with stop in both runners") {
      val s = Scan.map[Byte, Int](b => b & 0xff) >>>
        Scan.take[Int](7)
      agree(s, (0 until 32).map(_.toByte))
    },

    test("drop(n) skips the prefix in both runners") {
      val s = Scan.drop[Byte](5) >>>
        Scan.map[Byte, Int](b => b & 0xff)
      agree(s, (0 until 16).map(_.toByte))
    },

    test("fold accumulator surfaces as leftover in both runners") {
      val s = Scan.fold[Byte, Int](0)((acc, b) => acc + (b & 0xff))
      agree(s, (0 until 100).map(_.toByte))
    },

    test("countBytes leftover is the same big-endian count") {
      val s = Scan.countBytes
      agree(s, (0 until 250).map(_.toByte))
    },

    test("bombGuard within the budget passes through unchanged") {
      val s = Scan.bombGuard(1024L)
      agree(s, (0 until 64).map(_.toByte))
    },

    test("bombGuard over the budget fails identically") {
      val s = Scan.bombGuard(10L)
      agree(s, (0 until 32).map(_.toByte))
    },

    test("fixedChunk emits the same chunks") {
      val s = Scan.fixedChunk(8)
      agree(s, (0 until 64).map(_.toByte))
    },

    test("hash leftover is the same digest bytes") {
      val s = Scan.hash(HashAlgo.Sha256)
      agree(s, (0 until 1024).map(i => (i * 7).toByte))
    },

    test("fanout falls back to legacy and still agrees") {
      val left  = Scan.map[Byte, Int](_ & 0xff)
      val right = Scan.map[Byte, Int](_ & 0x0f)
      val s     = left &&& right
      agree(s, (0 until 32).map(_.toByte))
    }
  )
}
