package io.github.adrielc.kyopdf.pipeline

import kyo.*

/** Finalize typed aborts as well as exceptions with the pinned synchronous Kyo
  * interpreter. Re-emit the original failure only after the bracket completes.
  * The error type stays explicit across the bracket.
  */
private[pipeline] object SyncResource:
  def ensure[E: ConcreteTag]: Ensure[E] = new Ensure[E]
  final class Ensure[E: ConcreteTag]:
    def apply[A, S](release: => Any < (Sync & Abort[Throwable]))(body: => A < (Abort[E] & S)): A < (Abort[E] & Sync & S) =
      Sync.ensure(release)(Abort.run[E](body)).map(Abort.get(_))
