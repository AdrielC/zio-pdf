/*
 * Non-JMH microbench: ASCII line parsing — Kyo `Parse` vs scodec `Attempt`
 * vs `zio.prelude.fx.ZPure` via [[zio.scodec.stream.PureDecoder]].
 *
 * Lanes
 *   1. **Kyo** — `Parse.run` on `Text` (`separatedBy` / `repeat`).
 *   2. **scodec** — one strict `Decoder[Chunk[…]]` (single `toByteArray`, one pass).
 *   3. **ZPure** — `PureDecoder.once(same Decoder)` so scheduling is `ZPure`
 *      while the grammar lives in one scodec-shaped `Decoder` (the same
 *      pattern as lifting a strict decode into `StreamDecoder.fromPure`).
 *   4. **Hand** — byte-array scan, JIT reference.
 *
 * scodec and ZPure deliberately share identical **whole-buffer** decoders so
 * neither pays an accidental O(n²) “copy the remainder per line” tax; Kyo
 * stays incremental on `Text` (its natural model).
 *
 * Lower ms/iter is better. `@@ sequential` keeps stdout readable.
 */

package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.{Attempt, DecodeResult, Decoder, Err}
import _root_.scodec.bits.BitVector
import kyo.{Abort, Parse, Result as KResult, Text, *}
import zio.scodec.stream.PureDecoder
import zio.test.*
import zio.test.TestAspect.sequential

object ParsePerfBench extends ZIOSpecDefault {

  private val CS = StandardCharsets.US_ASCII

  private val simpleLines: Int  = 280_000
  private val complexLines: Int = 110_000

  private val simplePayload: String = {
    val sb = new java.lang.StringBuilder(simpleLines * 7)
    var i  = 0
    while i < simpleLines do {
      sb.append(f"$i%05d").append('\n')
      i += 1
    }
    sb.toString
  }

  private val complexPayload: String = {
    val sb = new java.lang.StringBuilder(complexLines * 18)
    var i  = 0
    while i < complexLines do {
      sb.append("obj ").append(i % 9999).append(' ').append(i % 7).append(" R\n")
      i += 1
    }
    sb.toString
  }

  private val simpleBits: BitVector = BitVector.view(simplePayload.getBytes(CS))
  private val complexBits: BitVector = BitVector.view(complexPayload.getBytes(CS))

  private val simpleBytes: Array[Byte] = simplePayload.getBytes(CS)
  private val complexBytes: Array[Byte] = complexPayload.getBytes(CS)

  /** Strict: consume the whole buffer into one `Chunk[Int]` (one `toByteArray`). */
  private val allSimpleIntsDec: Decoder[Chunk[Int]] = Decoder { bits =>
    if bits.size % 8 != 0 then Attempt.failure(Err("simple bench: require byte-aligned BitVector"))
    else
      decodeAllSimpleInts(bits.toByteArray).map(Chunk.from(_)).map(DecodeResult(_, BitVector.empty))
  }

  private def decodeAllSimpleInts(arr: Array[Byte]): Attempt[scala.collection.mutable.ArrayBuffer[Int]] = {
    val buf = scala.collection.mutable.ArrayBuffer[Int]()
    var i   = 0
    while i < arr.length do
      var j = i
      while j < arr.length && arr(j) != '\n'.toByte do j += 1
      if j >= arr.length then return Attempt.failure(Err("truncated int line"))
      val s = new String(arr, i, j - i, CS)
      s.toIntOption match {
        case None    => return Attempt.failure(Err(s"not an int: $s"))
        case Some(v) => buf += v
      }
      i = j + 1
    Attempt.successful(buf)
  }

  private def parseObjLineBytes(arr: Array[Byte], j0: Int): Either[Err, ((Int, Int), Int)] = {
    var j = j0
    val prefix = "obj ".getBytes(CS)
    var p = 0
    while p < prefix.length do
      if j >= arr.length || arr(j) != prefix(p) then return Left(Err("expected obj prefix"))
      j += 1
      p += 1
    var a = 0
    var d = 0
    while d < 10 && j < arr.length && arr(j) >= '0' && arr(j) <= '9' do
      a = a * 10 + (arr(j) - '0')
      j += 1
      d += 1
    if d == 0 || j >= arr.length || arr(j) != ' '.toByte then return Left(Err("expected space after first int"))
    j += 1
    var b = 0
    d = 0
    while d < 10 && j < arr.length && arr(j) >= '0' && arr(j) <= '9' do
      b = b * 10 + (arr(j) - '0')
      j += 1
      d += 1
    if d == 0 then return Left(Err("expected second int"))
    val tail = " R\n".getBytes(CS)
    p = 0
    while p < tail.length do
      if j >= arr.length || arr(j) != tail(p) then return Left(Err("expected  R\\n tail"))
      j += 1
      p += 1
    Right(((a, b), j))
  }

  private val allComplexObjsDec: Decoder[Chunk[(Int, Int)]] = Decoder { bits =>
    if bits.size % 8 != 0 then Attempt.failure(Err("complex bench: require byte-aligned BitVector"))
    else
      decodeAllComplexObjs(bits.toByteArray).map(Chunk.from(_)).map(DecodeResult(_, BitVector.empty))
  }

  private def decodeAllComplexObjs(
      arr: Array[Byte]
  ): Attempt[scala.collection.mutable.ArrayBuffer[(Int, Int)]] = {
    val buf = scala.collection.mutable.ArrayBuffer[(Int, Int)]()
    var i   = 0
    while i < arr.length do
      parseObjLineBytes(arr, i) match {
        case Left(e)          => return Attempt.failure(e)
        case Right((pair, j)) => buf += pair; i = j
      }
    Attempt.successful(buf)
  }

  private val zpSimpleOnce: PureDecoder[Chunk[Int]]            = PureDecoder.once(allSimpleIntsDec)
  private val zpComplexOnce: PureDecoder[Chunk[(Int, Int)]]    = PureDecoder.once(allComplexObjsDec)

  private val kyoSimple: Chunk[Int] < Parse =
    Parse.separatedBy(Parse.int, Parse.char('\n'), allowTrailing = true)

  private val kyoOneObj: (Int, Int) < Parse =
    for
      _ <- Parse.literal(Text("obj "))
      a <- Parse.int
      _ <- Parse.char(' ')
      b <- Parse.int
      _ <- Parse.literal(Text(" R\n"))
    yield (a, b)

  private val kyoComplex: Chunk[(Int, Int)] < Parse =
    Parse.repeat(kyoOneObj)

  private def handSimpleCount(arr: Array[Byte]): Int = {
    var i     = 0
    var lines = 0
    val n     = arr.length
    while i < n do {
      var v = 0
      var d = 0
      while i < n && arr(i) >= '0' && arr(i) <= '9' do {
        v = v * 10 + (arr(i) - '0')
        d += 1
        i += 1
      }
      if d == 0 || i >= n || arr(i) != '\n'.toByte then return lines
      i += 1
      lines += 1
    }
    lines
  }

  private def handComplexCount(arr: Array[Byte]): Int = {
    var i     = 0
    var lines = 0
    val n     = arr.length
    while i < n do {
      val need = "obj ".getBytes(CS)
      var k    = 0
      while k < need.length do {
        if i >= n || arr(i) != need(k) then return lines
        i += 1
        k += 1
      }
      var a = 0
      var d = 0
      while d < 10 && i < n && arr(i) >= '0' && arr(i) <= '9' do {
        a = a * 10 + (arr(i) - '0')
        i += 1
        d += 1
      }
      if d == 0 || i >= n || arr(i) != ' '.toByte then return lines
      i += 1
      var b = 0
      d = 0
      while d < 10 && i < n && arr(i) >= '0' && arr(i) <= '9' do {
        b = b * 10 + (arr(i) - '0')
        i += 1
        d += 1
      }
      if d == 0 then return lines
      val tail = " R\n".getBytes(CS)
      k = 0
      while k < tail.length do {
        if i >= n || arr(i) != tail(k) then return lines
        i += 1
        k += 1
      }
      lines += 1
    }
    lines
  }

  private def timeMillis(label: String, repeats: Int)(thunk: => Unit): Long = {
    var i = 0
    while i < 2 do { thunk; i += 1 }
    val t0 = java.lang.System.nanoTime()
    var j  = 0
    while j < repeats do { thunk; j += 1 }
    val ms = (java.lang.System.nanoTime() - t0) / 1_000_000L / repeats
    println(f"  [$label%-52s] ${ms}%5d ms / iter (avg of $repeats)")
    ms
  }

  def spec: Spec[Any, Any] =
    suite("ParsePerfBench")(
      test("simple: many `int\\n` lines (~ASCII decimal + LF)") {
        println(s"\n=== simple int+LF (${simplePayload.length} chars) ===")
        val kyoMs = timeMillis("Kyo Parse.separatedBy(int, '\\n')                    ", 5) {
          val r = Abort.run(Parse.run(Text(simplePayload))(kyoSimple)).eval
          r match {
            case KResult.Success(c) =>
              require(c.length == simpleLines)
              require(c(0) == 0 && c(simpleLines - 1) == simpleLines - 1)
            case other => throw new MatchError(other)
          }
        }
        val scMs = timeMillis("scodec Decoder[Chunk[Int]] (one pass)            ", 5) {
          val c = allSimpleIntsDec.decode(simpleBits).require.value
          require(c.length == simpleLines)
        }
        val zpMs = timeMillis("PureDecoder.once(Decoder)  [ZPure + same decode] ", 5) {
          zpSimpleOnce.decodeStrict(simpleBits) match {
            case Left(e) =>
              throw new RuntimeException(e.toString)
            case Right(dr) =>
              require(dr.value.length == 1)
              require(dr.value(0).length == simpleLines)
          }
        }
        val handMs = timeMillis("hand byte scan (reference)                       ", 5) {
          require(handSimpleCount(simpleBytes) == simpleLines)
        }
        println(f"  ratio Kyo/hand     = ${kyoMs.toDouble / math.max(1L, handMs)}%.2fx")
        println(f"  ratio scodec/hand  = ${scMs.toDouble / math.max(1L, handMs)}%.2fx")
        println(f"  ratio ZPure/hand   = ${zpMs.toDouble / math.max(1L, handMs)}%.2fx")
        assertTrue(kyoMs < 120_000L && scMs < 120_000L && zpMs < 120_000L && handMs < 120_000L)
      },
      test("complex: many `obj gen num R\\n` lines (PDF-ish indirect ref)") {
        println(s"\n=== complex obj lines (${complexPayload.length} chars) ===")
        val kyoMs = timeMillis("Kyo Parse.repeat(obj / gen / R line)             ", 5) {
          val r = Abort.run(Parse.run(Text(complexPayload))(kyoComplex)).eval
          r match {
            case KResult.Success(c) => require(c.length == complexLines)
            case other              => throw new MatchError(other)
          }
        }
        val scMs = timeMillis("scodec Decoder[Chunk[(Int,Int)]] (one pass)       ", 5) {
          val c = allComplexObjsDec.decode(complexBits).require.value
          require(c.length == complexLines)
        }
        val zpMs = timeMillis("PureDecoder.once(Decoder)  [ZPure + same decode]  ", 5) {
          zpComplexOnce.decodeStrict(complexBits) match {
            case Left(e) =>
              throw new RuntimeException(e.toString)
            case Right(dr) =>
              require(dr.value.length == 1)
              require(dr.value(0).length == complexLines)
          }
        }
        val handMs = timeMillis("hand byte scan (reference)                       ", 5) {
          require(handComplexCount(complexBytes) == complexLines)
        }
        println(f"  ratio Kyo/hand     = ${kyoMs.toDouble / math.max(1L, handMs)}%.2fx")
        println(f"  ratio scodec/hand  = ${scMs.toDouble / math.max(1L, handMs)}%.2fx")
        println(f"  ratio ZPure/hand   = ${zpMs.toDouble / math.max(1L, handMs)}%.2fx")
        assertTrue(kyoMs < 120_000L && scMs < 120_000L && zpMs < 120_000L && handMs < 120_000L)
      }
    ) @@ sequential
}
