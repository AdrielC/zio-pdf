/*
 * Diagnostic logging through [[ZPure]]'s log channel (`W = String`).
 *
 * Stream stages use `ZPure.log` for *outputs* (Decoded, TopLevel, …).
 * This object is for *diagnostic* lines only — duplicate-filter counts,
 * debug notes — accumulated with `runAll` and drained at ZIO or sync
 * boundaries.
 */

package zio.pdf

import zio.{Chunk, UIO, ZIO}
import zio.prelude.fx.ZPure

/** Alias for the ZPure log entry type (diagnostic lines). */
type ZPureLogEntry = String

object ZPureLog {

  def debug(message: => String): ZPure[ZPureLogEntry, Unit, Unit, Any, Nothing, Unit] =
    ZPure.log(message)

  def error(message: => String): ZPure[ZPureLogEntry, Unit, Unit, Any, Nothing, Unit] =
    ZPure.log(s"ERROR: $message")

  /** Accumulate diagnostic lines from a single message (pure, no effects). */
  def lines(message: => String): Chunk[ZPureLogEntry] =
    debug(message).runAll(())._1

  val empty: Chunk[ZPureLogEntry] = Chunk.empty

  def drainToZio(lines: Chunk[ZPureLogEntry]): UIO[Unit] =
    if (lines.isEmpty) ZIO.unit
    else ZIO.foreachDiscard(lines)(line => ZIO.log(line))

  def drainSync(lines: Chunk[ZPureLogEntry]): Unit =
    lines.foreach(line => java.lang.System.err.println(line))
}
