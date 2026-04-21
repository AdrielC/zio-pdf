package zio.graviton.scan

/**
 * Kyo's [[kyo.Abort.run]] requires a [[kyo.ConcreteTag]] for the failure type;
 * generic [[ScanDone]] cannot be used directly as `Abort`'s `E` parameter.
 * This wrapper boxes [[ScanDone]] behind a concrete reference type.
 */
final case class ScanAbort private (private val raw: AnyRef)

object ScanAbort {
  def apply[O, E](d: ScanDone[O, E]): ScanAbort =
    new ScanAbort(d.asInstanceOf[AnyRef])

  def unwrap[O, E](a: ScanAbort): ScanDone[O, E] =
    a.raw.asInstanceOf[ScanDone[O, E]]
}
