package zio.graviton.scan

enum ScanDone[+O, +E]:
  /** Input exhausted cleanly; leftover flushes buffered state downstream. */
  case Success(leftover: Seq[O])

  /** Scan stopped early (`take`, etc.). */
  case Stop(leftover: Seq[O])

  /** Scan failed; partial is what was emitted before failure. */
  case Failure(err: E, partial: Seq[O])

object ScanDone {
  def success[O]: ScanDone[O, Nothing]                 = Success(Seq.empty)
  def successWith[O](l: Seq[O]): ScanDone[O, Nothing] = Success(l)
  def stop[O]: ScanDone[O, Nothing]                    = Stop(Seq.empty)
  def stopWith[O](l: Seq[O]): ScanDone[O, Nothing]     = Stop(l)
  def fail[O, E](e: E): ScanDone[O, E]                 = Failure(e, Seq.empty)
  def failWith[O, E](e: E, p: Seq[O]): ScanDone[O, E] = Failure(e, p)
}
