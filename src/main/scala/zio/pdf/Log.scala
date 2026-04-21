/*
 * Replacement for fs2.pdf.Log. The legacy version sat on top of
 * cats-effect IO + log4cats; here we just thread a tiny ZIO-friendly
 * logger that defers entirely to zio.Console / zio.ZIO.logDebug.
 *
 * Most callers in the legacy code only used `debug`, so that's what
 * the trait exposes. `noop` keeps the existing API ergonomics; `live`
 * just routes to ZIO's logger.
 */

package zio.pdf

import zio.{UIO, ZIO}

trait Log {
  def debug(message: => String): UIO[Unit]
  def error(message: => String): UIO[Unit]
}

object Log {

  val noop: Log = new Log {
    def debug(message: => String): UIO[Unit] = ZIO.unit
    def error(message: => String): UIO[Unit] = ZIO.unit
  }

  /** Routes to ZIO's built-in logger (`ZIO.logDebug`/`ZIO.logError`). */
  val live: Log = new Log {
    def debug(message: => String): UIO[Unit] = ZIO.logDebug(message)
    def error(message: => String): UIO[Unit] = ZIO.logError(message)
  }
}
