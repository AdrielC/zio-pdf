package zio.graviton.scan

import kyo.*

/**
 * Scan program: pull inputs, emit outputs, complete via [[ScanTerminal]] (Kyo-safe `Abort` payload).
 * Typed completion [[ScanDone]] is recovered in [[ScanRunner]].
 */
type ScanProg[I, O, E, S] =
  Unit < (Poll[I] & Emit[O] & Abort[ScanTerminal] & S)
