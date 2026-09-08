/*
 * Free arrow AST over [[Pipe]] — volga `FreeCat` shape, folded to fused functions.
 *
 * Use when stages are assembled dynamically (config, plugins) and you still
 * want volga reassociation before interpretation.
 *
 * Reference: modules/volga/.../free/FreeCat.scala
 */

package zio.pdf.pipe

sealed trait FreePipe[-A, +B] extends Product with Serializable

object FreePipe {

  final case class Id[A]() extends FreePipe[A, A]

  final case class Embed[A, B](pipe: Pipe[A, B]) extends FreePipe[A, B]

  final case class Seq[A, M, B](left: FreePipe[A, M], right: FreePipe[M, B])
      extends FreePipe[A, B]

  final case class Par[A, C, B, D](left: FreePipe[A, B], right: FreePipe[C, D])
      extends FreePipe[(A, C), (B, D)]

  final case class Fan[A, OL, OR](left: FreePipe[A, OL], right: FreePipe[A, OR])
      extends FreePipe[A, (OL, OR)]

  final case class Ch[A, C, B, D](left: FreePipe[A, B], right: FreePipe[C, D])
      extends FreePipe[Either[A, C], Either[B, D]]

  /** Routed merge to one output (volga `sum` / `|||`). */
  final case class Merge[A, C, B](left: FreePipe[A, B], right: FreePipe[C, B])
      extends FreePipe[Either[A, C], B]

  def lift[A, B](pipe: Pipe[A, B]): FreePipe[A, B] = Embed(pipe)

  def arr[A, B](f: A => B): FreePipe[A, B] = Embed(Pipe(f))

  /** Flatten left-nested `Seq` chains (volga `Sequential` reassociation). */
  def flatten[A, B](fp: FreePipe[A, B]): FreePipe[A, B] = fp match {
    case Seq(l, r) =>
      (flatten(l), flatten(r)) match {
        case (Seq(l2, m), r2) => Seq(l2, Seq(m, r2))
        case (l2, r2)         => Seq(l2, r2)
      }
    case other => other
  }

  /** Interpret the free arrow as a single fused [[Pipe]]. */
  def fold[A, B](fp: FreePipe[A, B]): Pipe[A, B] = flatten(fp) match {
    case Id()           => Pipe.id
    case Embed(p)       => p
    case Seq(l, r)      => fold(l) >>> fold(r)
    case Par(l, r)      => fold(l) *** fold(r)
    case Fan(l, r)      => fold(l) &&& fold(r)
    case Ch(l, r)       => PipeArrow.choose(fold(l), fold(r))
    case Merge(l, r)    => Pipe.sum(fold(l), fold(r))
  }

  extension [A, B](self: FreePipe[A, B]) {
    infix def >>>[C](right: FreePipe[B, C]): FreePipe[A, C]             = Seq(self, right)
    infix def &&&[C](right: FreePipe[A, C]): FreePipe[A, (B, C)]       = Fan(self, right)
    infix def ***[C, D](right: FreePipe[C, D]): FreePipe[(A, C), (B, D)] = Par(self, right)
    infix def |||[C](right: FreePipe[C, B]): FreePipe[Either[A, C], B] = Merge(self, right)
    def run(a: A): B                                                     = fold(self).run(a)
  }
}
