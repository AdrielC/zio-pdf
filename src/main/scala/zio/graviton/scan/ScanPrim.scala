package zio.graviton.scan

import kyo.Chunk as KChunk

/**
 * Serializable scan primitive algebra (no closures in named constructors beyond
 * [[Map]] / [[Filter]] — full `Schema` for those awaits `SchemaExpr`; see design doc).
 */
sealed trait ScanPrim[-I, +Out]

object ScanPrim {

  case class Map[I, O](f: I => O) extends ScanPrim[I, One[O]]

  case class Filter[A](p: A => Boolean) extends ScanPrim[A, Null | One[A]]

  /** Emits at most `n` inputs as [[One]], then signals [[ScanDone.Stop]] via [[Abort]]. */
  case class Take[A](n: Int) extends ScanPrim[A, One[A]]

  /** Accumulates digest; per-step output is [[Null]]; digest appears in [[ScanDone.Success]]. */
  case class Hash(algo: HashAlgo) extends ScanPrim[Byte, Null]

  /** Counts bytes; per-step [[Null]]; count in [[ScanDone.Success]] as `Seq(Long)` — see note. */
  case object CountBytes extends ScanPrim[Byte, Null]

  case class BombGuard(maxBytes: Long) extends ScanPrim[Byte, One[Byte]]

  case class FastCDC(min: Int, avg: Int, max: Int) extends ScanPrim[Byte, Null | NonEmpty[KChunk[Byte]]]

  case class FixedChunk(n: Int) extends ScanPrim[Byte, Null | NonEmpty[KChunk[Byte]]]

  def identity[A]: ScanPrim[A, One[A]] = Map(Predef.identity)
}
