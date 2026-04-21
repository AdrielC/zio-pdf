package zio.graviton.scan

/**
 * Kyo `Abort` payload without type parameters so [[kyo.ConcreteTag]] can be derived.
 * Typed [[ScanDone]] remains the logical model for pure evaluation; convert at boundaries.
 */
enum ScanTerminal:
  case Success(leftover: Seq[Any])
  case Stop(leftover: Seq[Any])
  case Failure(err: Any, partial: Seq[Any])

object ScanTerminal:
  def success: ScanTerminal = Success(Seq.empty)

  def successWith(leftover: Seq[Any]): ScanTerminal = Success(leftover)

  def stop: ScanTerminal = Stop(Seq.empty)

  def stopWith(leftover: Seq[Any]): ScanTerminal = Stop(leftover)

  def fail(err: Any): ScanTerminal = Failure(err, Seq.empty)

  def failWith(err: Any, partial: Seq[Any]): ScanTerminal = Failure(err, partial)

  def fromTyped[O, E](d: ScanDone[O, E]): ScanTerminal =
    d match
      case ScanDone.Success(l)    => Success(l)
      case ScanDone.Stop(l)       => Stop(l)
      case ScanDone.Failure(e, p) => Failure(e, p)

  def toTyped[O, E](t: ScanTerminal): ScanDone[O, E] =
    t match
      case Success(l)    => ScanDone.Success(l.asInstanceOf[Seq[O]])
      case Stop(l)       => ScanDone.Stop(l.asInstanceOf[Seq[O]])
      case Failure(e, p) => ScanDone.Failure(e.asInstanceOf[E], p.asInstanceOf[Seq[O]])
