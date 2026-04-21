package zio.graviton.scan

import java.security.MessageDigest
import kyo.Chunk

/** Scan primitive algebra. JVM function fields are not schema-serializable yet (see SchemaExpr). */
sealed trait ScanPrim[-I, +Out]

object ScanPrim:

  case class Map[I, O](f: I => O) extends ScanPrim[I, One[O]]

  case class Filter[A](p: A => Boolean) extends ScanPrim[A, Null | One[A]]

  case class Take[A](n: Int) extends ScanPrim[A, One[A]]

  case class Hash(algo: HashAlgo) extends ScanPrim[Byte, Null]

  case object CountBytes extends ScanPrim[Byte, Null]

  case class BombGuard(maxBytes: Long) extends ScanPrim[Byte, One[Byte]]

  case class FastCDC(min: Int, avg: Int, max: Int) extends ScanPrim[Byte, Null | NonEmpty[Byte]]

  case class FixedChunk(n: Int) extends ScanPrim[Byte, Null | NonEmpty[Byte]]

  def identity[A]: ScanPrim[A, One[A]] = Map(a => a)

  private[scan] def evalMap[I, O](p: Map[I, O], i: I): One[O] = One(p.f(i))

  private[scan] def evalFilter[A](p: Filter[A], a: A): Null | One[A] =
    if p.p(a) then One(a) else Null

  private[scan] def evalTake[A](remaining: Int, a: A): (Int, StepOut[A]) =
    if remaining <= 0 then (0, Null)
    else if remaining == 1 then (0, One(a))
    else (remaining - 1, One(a))

  private[scan] def evalBombGuard(p: BombGuard, seen: Long, b: Byte): Either[BombError, One[Byte]] =
    val n = seen + 1
    if n > p.maxBytes then Left(BombError(n, p.maxBytes))
    else Right(One(b))

  private[scan] def evalFixedChunk(
      n: Int,
      buf: Array[Byte],
      bufLen: Int,
      b: Byte
  ): (Array[Byte], Int, StepOut[Byte]) =
    if n <= 0 then throw new IllegalArgumentException("FixedChunk n must be positive")
    if bufLen < n - 1 then
      buf(bufLen) = b
      (buf, bufLen + 1, Null)
    else
      buf(bufLen) = b
      val chunk = Chunk.from(java.util.Arrays.copyOf(buf, n))
      val rest = new Array[Byte](n)
      System.arraycopy(buf, n, rest, 0, bufLen + 1 - n)
      (rest, bufLen + 1 - n, NonEmpty(chunk))

  private[scan] def flushFixedChunk(buf: Array[Byte], bufLen: Int): NonEmpty[Byte] | Null =
    if bufLen == 0 then Null
    else NonEmpty(Chunk.from(java.util.Arrays.copyOf(buf, bufLen)))

  private[scan] def evalFastCdc(
      min: Int,
      avg: Int,
      max: Int,
      buf: Array[Byte],
      bufLen: Int,
      b: Byte
  ): (Array[Byte], Int, StepOut[Byte]) =
    if bufLen == max then
      val forced = java.util.Arrays.copyOf(buf, max)
      val nb     = new Array[Byte](max)
      nb(0) = b
      (nb, 1, NonEmpty(Chunk.from(forced)))
    else
      buf(bufLen) = b
      val nlen = bufLen + 1
      if nlen < min then (buf, nlen, Null)
      else
        val window = java.util.Arrays.copyOf(buf, math.min(nlen, max))
        val cut    = zio.pdf.cdc.FastCdc.nextCut(window, min, avg, max)
        if cut < nlen then
          val chunk = java.util.Arrays.copyOfRange(buf, 0, cut)
          val rest  = new Array[Byte](max)
          val rem   = nlen - cut
          System.arraycopy(buf, cut, rest, 0, rem)
          (rest, rem, NonEmpty(Chunk.from(chunk)))
        else
          (buf, nlen, Null)

  private[scan] def flushFastCdc(buf: Array[Byte], bufLen: Int): NonEmpty[Byte] | Null =
    if bufLen == 0 then Null
    else NonEmpty(Chunk.from(java.util.Arrays.copyOf(buf, bufLen)))

  private[scan] def newDigest(algo: HashAlgo): MessageDigest =
    MessageDigest.getInstance(algo.digestAlgorithm)
