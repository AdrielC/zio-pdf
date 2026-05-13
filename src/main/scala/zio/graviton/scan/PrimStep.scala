package zio.graviton.scan

import java.security.MessageDigest
import java.nio.ByteBuffer
import kyo.Chunk as KChunk
import zio.pdf.cdc.FastCdc

/**
 * One-step evaluation of [[ScanPrim]] with explicit mutable state boxes
 * (direct interpreter — no Kyo in the hot path).
 */
private[scan] object PrimStep {

  sealed trait State

  final class MapState extends State

  final class FilterState extends State

  final class TakeState(var remaining: Int) extends State

  final class BombState(var seen: Long, val limit: Long) extends State

  final class HashState(val md: MessageDigest) extends State

  final class CountState(var n: Long) extends State

  final class CdcState(var buffer: Array[Byte], val cfg: FastCdc.Config) extends State

  final class FixedState(var buf: Array[Byte], val n: Int) extends State {
    require(n > 0)
  }

  enum StepVal[+M] {
    case Ok(st: State, outs: List[StepOut[M]])
    case BombFail(e: BombError)
  }

  def initState(p: ScanPrim[?, ?]): State =
    p match {
      case _: ScanPrim.Map[?, ?]  => new MapState
      case _: ScanPrim.Filter[?] => new FilterState
      case ScanPrim.Take(k)      => new TakeState(k)
      case ScanPrim.BombGuard(lim) =>
        new BombState(0L, lim)
      case ScanPrim.Hash(algo) => new HashState(newDigest(algo))
      case ScanPrim.CountBytes => new CountState(0L)
      case ScanPrim.FastCDC(min, avg, max) =>
        new CdcState(new Array[Byte](0), FastCdc.Config(minSize = min, avgSize = avg, maxSize = max))
      case ScanPrim.FixedChunk(n) =>
        new FixedState(new Array[Byte](0), n)
    }

  private def newDigest(algo: HashAlgo): MessageDigest =
    algo match {
      case HashAlgo.Sha256 => MessageDigest.getInstance("SHA-256")
      case HashAlgo.Blake3 =>
        throw new UnsupportedOperationException(
          "Blake3 is not on the JDK MessageDigest SPI in this build; use Sha256 or add a Blake3 provider."
        )
    }

  private inline def single[M](out: StepOut[M]): List[StepOut[M]] =
    if out == null then Nil else List(out)

  def step[I, M](
    p: ScanPrim[I, StepOut[M]],
    st: State,
    input: I
  ): StepVal[M] =
    p match {
      case ScanPrim.Map(f) =>
        val _ = st.asInstanceOf[MapState]
        StepVal.Ok(st, single(One(f(input).asInstanceOf[M])))

      case ScanPrim.Filter(pred) =>
        val _ = st.asInstanceOf[FilterState]
        if pred.asInstanceOf[Any => Boolean](input) then StepVal.Ok(st, single(One(input.asInstanceOf[M])))
        else StepVal.Ok(st, Nil)

      case ScanPrim.Take(_) =>
        val ts = st.asInstanceOf[TakeState]
        if ts.remaining <= 0 then StepVal.Ok(st, Nil)
        else {
          ts.remaining -= 1
          StepVal.Ok(st, single(One(input.asInstanceOf[M])))
        }

      case ScanPrim.BombGuard(_) =>
        val bs = st.asInstanceOf[BombState]
        bs.seen += 1L
        if bs.seen > bs.limit then StepVal.BombFail(BombError(bs.seen, bs.limit))
        else StepVal.Ok(st, single(One(input.asInstanceOf[M])))

      case ScanPrim.Hash(_) =>
        val hs = st.asInstanceOf[HashState]
        hs.md.update(input.asInstanceOf[Byte])
        StepVal.Ok(st, Nil)

      case ScanPrim.CountBytes =>
        val cs = st.asInstanceOf[CountState]
        cs.n += 1L
        StepVal.Ok(st, Nil)

      case ScanPrim.FastCDC(_, _, _) =>
        val cdc = st.asInstanceOf[CdcState]
        val b            = input.asInstanceOf[Byte]
        val merged       = FastCdc.mergeArrays(cdc.buffer, Array(b))
        val (segs, rest) = FastCdc.drainToArrays(merged, flushTail = false, cdc.cfg)
        cdc.buffer = rest
        val outs =
          segs.iterator.map(arr => KChunk.from(arr).asInstanceOf[StepOut[M]]).toList
        StepVal.Ok(st, outs)

      case ScanPrim.FixedChunk(_) =>
        val fs = st.asInstanceOf[FixedState]
        val b = input.asInstanceOf[Byte]
        fs.buf = fs.buf :+ b
        if fs.buf.length < fs.n then StepVal.Ok(st, Nil)
        else {
          val chunk = KChunk.from(fs.buf)
          fs.buf = new Array[Byte](0)
          StepVal.Ok(st, List(chunk.asInstanceOf[StepOut[M]]))
        }
    }

  def flush[I, M](p: ScanPrim[I, StepOut[M]], st: State): List[StepOut[M]] =
    p match {
      case ScanPrim.Hash(_) =>
        val hs = st.asInstanceOf[HashState]
        single(KChunk.from(hs.md.digest()).asInstanceOf[StepOut[M]])

      case ScanPrim.CountBytes =>
        val cs = st.asInstanceOf[CountState]
        single(KChunk.from(ByteBuffer.allocate(8).putLong(cs.n).array()).asInstanceOf[StepOut[M]])

      case ScanPrim.FastCDC(_, _, _) =>
        val cdc = st.asInstanceOf[CdcState]
        val (segs, _) = FastCdc.drainToArrays(cdc.buffer, flushTail = true, cdc.cfg)
        cdc.buffer = new Array[Byte](0)
        if segs.isEmpty then Nil
        else segs.iterator.map(arr => KChunk.from(arr).asInstanceOf[StepOut[M]]).toList

      case ScanPrim.FixedChunk(_) =>
        val fs = st.asInstanceOf[FixedState]
        if fs.buf.isEmpty then Nil
        else {
          val ch = KChunk.from(fs.buf)
          fs.buf = new Array[Byte](0)
          List(ch.asInstanceOf[StepOut[M]])
        }

      case _ =>
        Nil
    }

  def takeExhausted(st: State): Boolean =
    st match {
      case ts: TakeState => ts.remaining <= 0
      case _             => false
    }
}
