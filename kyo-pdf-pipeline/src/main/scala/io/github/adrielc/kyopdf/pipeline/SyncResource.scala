package io.github.adrielc.kyopdf.pipeline

import kyo.*

/** Finalize typed aborts as well as exceptions with the pinned synchronous Kyo
  * interpreter. Re-emit the original failure only after the bracket completes.
  * The cast preserves the caller's original effect row, as in Kyo Scope.run;
  * it does not turn failures into values or remove an effect from that row.
  */
private[pipeline] object SyncResource:
  def ensure[A, S](release: => Any < (Sync & Abort[Throwable]))(body: => A < S): A < (Sync & S) =
    Sync.ensure(release)(Abort.run[Any](body)).map { result =>
      Abort.get(result.asInstanceOf[Result[Nothing, A]])
    }
