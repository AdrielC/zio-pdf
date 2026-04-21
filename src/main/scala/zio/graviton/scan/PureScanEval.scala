package zio.graviton.scan

import java.security.MessageDigest
import kyo.Chunk

/** Pure evaluation of [[FreeScan]] (buffered pipeline + spine flattening). */
object PureScanEval:

  final case class Result[O, E](done: ScanDone[O, E], emitted: List[O])

  def runDirect[I, O](scan: FreeScan[I, O], inputs: Iterable[I]): List[O] =
    runWithDone(scan, inputs).emitted

  def runWithDone[I, O, E](scan: FreeScan[I, O], inputs: Iterable[I]): Result[O, E] =
    scan match
      case f: FreeScan.Fanout[I, ol, or] =>
        val pairs = inputs.iterator.flatMap { i =>
          val lo = runDirect(f.l, List(i))
          val ro = runDirect(f.r, List(i))
          if lo.nonEmpty && ro.nonEmpty then Iterator.single((lo.head, ro.head))
          else Iterator.empty
        }.toList
        Result(ScanDone.success[(ol, or)], pairs)
      case c: FreeScan.Choice[il, ir, o] =>
        val outs = inputs.iterator.flatMap {
          case Left(il)  => runDirect(c.l, List(il)).iterator
          case Right(ir) => runDirect(c.r, List(ir)).iterator
        }.toList
        Result(ScanDone.success[o], outs)
      case _ =>
        val buf = scala.collection.mutable.ListBuffer.empty[O]
        val machine = new Machine
        val done = runBuffered(scan, inputs.iterator, buf, machine)
        Result(done, buf.toList)

  private final class Machine:
    var takeRemaining: Int = -1
    var bombSeen: Long = 0L
    var countBytes: Long = 0L
    var digest: MessageDigest = null
    var fixedBuf: Array[Byte] = null
    var fixedLen: Int = 0
    var fixedN: Int = 0
    var cdcBuf: Array[Byte] = null
    var cdcLen: Int = 0
    var cdcMin: Int = 0
    var cdcAvg: Int = 0
    var cdcMax: Int = 0

  private def runBuffered[I, O, E](
      scan: FreeScan[I, O],
      inputs: Iterator[I],
      out: scala.collection.mutable.ListBuffer[O],
      machine: Machine
  ): ScanDone[O, E] =
    Fusion.tryFuse(scan) match
      case Some(f) =>
        val g = f.asInstanceOf[I => O]
        inputs.foreach(i => out += g(i))
        ScanDone.success[O]
      case None =>
        Flatten.flattenSpine(scan) match
          case (SpineHead.FromFan(_), _) | (SpineHead.FromChoice(_), _) =>
            ScanDone.success[O] // unreachable: handled at top-level
          case (head, tail) =>
            val buffer = scala.collection.mutable.ArrayDeque.empty[Any]

            def flushHead: ScanDone[Any, Any] =
              head match
                case SpineHead.FromPrim(p) => flushPrim(p.op, machine)
                case _                     => ScanDone.success[Any]

            def stepHeadVal(i: I): Either[ScanDone[Any, Any], Any] =
              head match
                case SpineHead.FromArr(a) => Right(a.f(i))
                case SpineHead.FromPrim(p) =>
                  stepPrimHead(p.op, i, machine) match
                    case Left(d)  => Left(d)
                    case Right(v) => Right(v)
                case SpineHead.FromFan(f) =>
                  val lo = runDirect(f.l, List(i))
                  val ro = runDirect(f.r, List(i))
                  if lo.nonEmpty && ro.nonEmpty then Right((lo.head, ro.head))
                  else Right(Null)
                case SpineHead.FromChoice(c) =>
                  i match
                    case Left(il: Any) =>
                      val o = runDirect(c.l, List(il.asInstanceOf[Nothing]))
                      if o.nonEmpty then Right(o.head) else Right(Null)
                    case Right(ir: Any) =>
                      val o = runDirect(c.r, List(ir.asInstanceOf[Nothing]))
                      if o.nonEmpty then Right(o.head) else Right(Null)
                    case _ => Right(Null)

            def feedTailFinal(done: ScanDone[Any, Any]): ScanDone[O, E] =
              done match
                case ScanDone.Success(left) =>
                  left.foreach(buffer.append)
                  drainBufferToEnd(ScanDone.success[O].asInstanceOf[ScanDone[O, E]])
                case ScanDone.Stop(left) =>
                  left.foreach(buffer.append)
                  drainBufferToEnd(ScanDone.stop[O].asInstanceOf[ScanDone[O, E]])
                case ScanDone.Failure(err, partial) =>
                  partial.foreach(buffer.append)
                  drainBufferToFailure(err)

            def drainBufferToEnd(finalDone: ScanDone[O, E]): ScanDone[O, E] =
              @scala.annotation.tailrec
              def loop(): ScanDone[O, E] =
                if buffer.isEmpty then finalDone
                else
                  runTail(buffer.removeHead(), tail, out, new Machine) match
                    case TailOk.Continue     => loop()
                    case TailOk.Failed(d)    => d.asInstanceOf[ScanDone[O, E]]
                    case TailOk.EarlyStop(d) => d.asInstanceOf[ScanDone[O, E]]
              loop()

            def drainBufferToFailure[EE](err: EE): ScanDone[O, E] =
              @scala.annotation.tailrec
              def loop(): ScanDone[O, E] =
                if buffer.isEmpty then ScanDone.failWith[O, EE](err, out.toSeq).asInstanceOf[ScanDone[O, E]]
                else
                  runTail(buffer.removeHead(), tail, out, new Machine) match
                    case TailOk.Continue     => loop()
                    case TailOk.Failed(d)    => d.asInstanceOf[ScanDone[O, E]]
                    case TailOk.EarlyStop(d) => d.asInstanceOf[ScanDone[O, E]]
              loop()

            def pushIntermediate(v: Any): Option[ScanDone[O, E]] =
              v match
                case null => None
                case c: Chunk[?] =>
                  val ch = c.asInstanceOf[Chunk[Any]]
                  var i = 0
                  while i < ch.length do
                    buffer.append(ch(i))
                    i += 1
                  None
                case one =>
                  buffer.append(one)
                  None

            @scala.annotation.tailrec
            def pump(): ScanDone[O, E] =
              if buffer.nonEmpty then
                runTail(buffer.removeHead(), tail, out, machine) match
                  case TailOk.Continue     => pump()
                  case TailOk.Failed(d)    => d.asInstanceOf[ScanDone[O, E]]
                  case TailOk.EarlyStop(d) => d.asInstanceOf[ScanDone[O, E]]
              else if inputs.hasNext then
                val i = inputs.next()
                stepHeadVal(i) match
                  case Left(d) => feedTailFinal(d)
                  case Right(v) =>
                    pushIntermediate(v) match
                      case Some(done) => done
                      case None        => pump()
              else feedTailFinal(flushHead)

            pump()

  private enum TailOk:
    case Continue
    case Failed(done: ScanDone[Any, Any])
    case EarlyStop(done: ScanDone[Any, Any])

  private def runTail[O](
      m: Any,
      tail: List[FreeScan[?, O]],
      out: scala.collection.mutable.ListBuffer[O],
      machine: Machine
  ): TailOk =
    tail match
      case Nil =>
        out += m.asInstanceOf[O]
        TailOk.Continue
      case (h :: t) =>
        h match
          case a: FreeScan.Arr[Any, O] @unchecked =>
            runTail(a.f(m), t, out, machine)
          case p: FreeScan.Prim[Any, ?] @unchecked =>
            stepPrimMiddle(p.op, m, machine) match
              case Left(d) => TailOk.EarlyStop(d)
              case Right(v) =>
                v match
                  case null => TailOk.Continue
                  case c: Chunk[?] =>
                    val ch = c.asInstanceOf[Chunk[Any]]
                    var i = 0
                    var acc = TailOk.Continue
                    while i < ch.length && acc == TailOk.Continue do
                      acc = runTail(ch(i), t, out, machine)
                      i += 1
                    acc
                  case x => runTail(x, t, out, machine)
          case nested =>
            val sub = nested.asInstanceOf[FreeScan[Any, O]]
            val r = runWithDone(sub, List(m))
            r.done match
              case ScanDone.Failure(err, partial) =>
                partial.foreach(x => discard(runTail(x, t, out, new Machine)))
                TailOk.Failed(ScanDone.failWith(err, out.toSeq))
              case ScanDone.Stop(s) =>
                s.foreach(x => discard(runTail(x, t, out, new Machine)))
                TailOk.EarlyStop(ScanDone.stopWith(out.toSeq))
              case ScanDone.Success(s) =>
                s.foreach(x => discard(runTail(x, t, out, new Machine)))
                r.emitted.foreach(o => out += o)
                TailOk.Continue

  private inline def discard[A](inline a: A): Unit = ()

  private def stepPrimHead[I](op: ScanPrim[I, ?], i: I, machine: Machine): Either[ScanDone[Any, Any], Any] =
    op match
      case m: ScanPrim.Map[I, o] => Right(ScanPrim.evalMap(m, i))
      case f: ScanPrim.Filter[I] => Right(ScanPrim.evalFilter(f, i))
      case t: ScanPrim.Take[I] =>
        if machine.takeRemaining < 0 then machine.takeRemaining = t.n
        val (rem, outv) = ScanPrim.evalTake(machine.takeRemaining, i)
        machine.takeRemaining = rem
        if rem == 0 && outv == Null then Left(ScanDone.stopWith[I](Seq.empty))
        else Right(outv)
      case b: ScanPrim.BombGuard =>
        val bb = i.asInstanceOf[Byte]
        ScanPrim.evalBombGuard(b, machine.bombSeen, bb) match
          case Left(err) => Left(ScanDone.failWith[Byte, BombError](err, Seq.empty))
          case Right(one) =>
            machine.bombSeen += 1
            Right(one)
      case ScanPrim.CountBytes =>
        machine.countBytes += 1
        Right(Null)
      case h: ScanPrim.Hash =>
        if machine.digest == null then machine.digest = ScanPrim.newDigest(h.algo)
        machine.digest.update(i.asInstanceOf[Byte])
        Right(Null)
      case fc: ScanPrim.FastCDC =>
        initCdc(machine, fc)
        val b = i.asInstanceOf[Byte]
        val (nb, nl, outv) =
          ScanPrim.evalFastCdc(machine.cdcMin, machine.cdcAvg, machine.cdcMax, machine.cdcBuf, machine.cdcLen, b)
        machine.cdcBuf = nb
        machine.cdcLen = nl
        Right(outv)
      case fc: ScanPrim.FixedChunk =>
        initFixed(machine, fc)
        val b = i.asInstanceOf[Byte]
        val (nb, nl, outv) = ScanPrim.evalFixedChunk(machine.fixedN, machine.fixedBuf, machine.fixedLen, b)
        machine.fixedBuf = nb
        machine.fixedLen = nl
        Right(outv)

  private def initCdc(machine: Machine, fc: ScanPrim.FastCDC): Unit =
    if machine.cdcBuf == null then
      machine.cdcBuf = new Array[Byte](fc.max)
      machine.cdcMin = fc.min
      machine.cdcAvg = fc.avg
      machine.cdcMax = fc.max
      machine.cdcLen = 0

  private def initFixed(machine: Machine, fc: ScanPrim.FixedChunk): Unit =
    if machine.fixedBuf == null then
      machine.fixedBuf = new Array[Byte](fc.n)
      machine.fixedN = fc.n
      machine.fixedLen = 0

  private def flushPrim(op: ScanPrim[?, ?], machine: Machine): ScanDone[Any, Any] =
    op match
      case ScanPrim.CountBytes => ScanDone.successWith[Any](Seq(machine.countBytes))
      case h: ScanPrim.Hash =>
        if machine.digest == null then machine.digest = ScanPrim.newDigest(h.algo)
        ScanDone.successWith[Any](machine.digest.digest().toSeq)
      case fc: ScanPrim.FastCDC =>
        if machine.cdcBuf == null then ScanDone.success[Any]
        else
          ScanPrim.flushFastCdc(machine.cdcBuf, machine.cdcLen) match
            case null => ScanDone.success[Any]
            case ne    => ScanDone.successWith[Any](Seq(ne))
      case fc: ScanPrim.FixedChunk =>
        if machine.fixedBuf == null then ScanDone.success[Any]
        else
          ScanPrim.flushFixedChunk(machine.fixedBuf, machine.fixedLen) match
            case null => ScanDone.success[Any]
            case ne   => ScanDone.successWith[Any](Seq(ne))
      case _ => ScanDone.success[Any]

  private def stepPrimMiddle(op: ScanPrim[Any, ?], m: Any, machine: Machine): Either[ScanDone[Any, Any], Any] =
    stepPrimHead(op, m, machine)
