package zio.graviton.scan

import scala.collection.mutable

object DirectScanRun {

  sealed trait RunOutcome[+O]

  case class Done[O](values: List[O]) extends RunOutcome[O]

  case class Failed[O](err: BombError, partial: List[O]) extends RunOutcome[O]

  /** Fast path: pure `Map` / `Arr` chains collapse to one function. */
  def runDirect[I, O](scan: FreeScan[I, O], inputs: Iterable[I]): List[O] =
    Fusion.tryFuse(scan) match {
      case Some(f) => inputs.iterator.map(f).toList
      case None =>
        runLinear(linearize(scan), inputs.iterator) match {
          case Done(vs)       => vs
          case Failed(e, pre) => throw new ScanExecutionException(e, pre)
        }
    }

  def runDirectOutcome[I, O](scan: FreeScan[I, O], inputs: Iterable[I]): RunOutcome[O] =
    Fusion.tryFuse(scan) match {
      case Some(f) => Done(inputs.iterator.map(f).toList)
      case None    => runLinear(linearize(scan), inputs.iterator)
    }

  final class ScanExecutionException(val error: BombError, val partial: List[?])
      extends RuntimeException(s"BombGuard exceeded: seen=${error.seen} limit=${error.limit}")

  private def linearize[I, O](fs: FreeScan[I, O]): List[ScanPrim[Any, StepOut[Any]]] =
    fs match {
      case FreeScan.Prim(p) =>
        List(p.asInstanceOf[ScanPrim[Any, StepOut[Any]]])
      case FreeScan.Arr(f) =>
        List(ScanPrim.Map(f.asInstanceOf[Any => Any]))
      case FreeScan.AndThen(l, r) =>
        linearize(l.asInstanceOf[FreeScan[I, Any]]) ++ linearize(r.asInstanceOf[FreeScan[Any, O]])
      case FreeScan.Fanout(left, right) =>
        throw new UnsupportedOperationException(
          "Fanout requires runFanout — use DirectScanRun.runFanout or split pipelines."
        )
      case FreeScan.Choice(_, _) =>
        throw new UnsupportedOperationException("Choice is not supported by the linear runner.")
    }

  private def runLinear[I, O](
    prims: List[ScanPrim[Any, StepOut[Any]]],
    inputs: Iterator[I]
  ): RunOutcome[O] = {
    val n = prims.length
    if n == 0 then return Done(Nil)

    val states = prims.map(PrimStep.initState).toArray
    val acc    = mutable.ListBuffer.empty[O]

    def feed(stage: Int, v: Any): Unit =
      if stage >= n then acc += v.asInstanceOf[O]
      else
        PrimStep.step(prims(stage), states(stage), v) match {
          case PrimStep.StepVal.BombFail(e) =>
            throw new ScanExecutionException(e, acc.toList)
          case PrimStep.StepVal.Ok(_, outs) =>
            var i = 0
            while i < outs.length do {
              val o = outs(i)
              if o != null then {
                val kc = StepOut.toKChunk(o.asInstanceOf[StepOut[Any]])
                var j = 0
                while j < kc.length do {
                  feed(stage + 1, kc(j))
                  j += 1
                }
              }
              i += 1
            }
        }

    def flushChain(stage: Int): Unit =
      if stage >= n then ()
      else {
        val outs = PrimStep.flush(prims(stage), states(stage))
        var i    = 0
        while i < outs.length do {
          val o = outs(i)
          if o != null then {
            val kc = StepOut.toKChunk(o.asInstanceOf[StepOut[Any]])
            var j = 0
            while j < kc.length do {
              feed(stage + 1, kc(j))
              j += 1
            }
          }
          i += 1
        }
        flushChain(stage + 1)
      }

    try {
      var earlyStop = false
      while inputs.hasNext && !earlyStop do {
        val in = inputs.next()
        feed(0, in)
        if n > 0 && PrimStep.takeExhausted(states(0)) then earlyStop = true
      }
      flushChain(0)
      Done(acc.toList)
    } catch {
      case e: ScanExecutionException =>
        Failed(e.error.asInstanceOf[BombError], e.partial.asInstanceOf[List[O]])
    }
  }

  def runFanout[I, OL, OR](
    scan: FreeScan[I, (OL, OR)],
    inputs: Iterable[I]
  ): List[(OL, OR)] =
    scan match {
      case FreeScan.Fanout(l, r) =>
        val ol = runDirect(l.asInstanceOf[FreeScan[I, OL]], inputs)
        val or = runDirect(r.asInstanceOf[FreeScan[I, OR]], inputs)
        if ol.length != or.length then
          throw new IllegalStateException("Fanout branch output size mismatch")
        ol.zip(or)
      case _ =>
        runDirect(scan, inputs) // fused path
    }
}
