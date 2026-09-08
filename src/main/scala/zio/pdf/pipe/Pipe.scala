/*
 * Fused pure morphisms. Operators mirror volga / FreeScan arrows:
 *   >>>  compose      >< / ***  tensor     <> / &&&  fanout
 *   |||  choice       +++      disjoint parallel
 *
 * Full [[CartesianCat]] instance: [[PipeCat.pipeCartesian]].
 * Full [[CocartesianCat]] / [[DistributiveCat]]: [[PipeCat]].
 * volga `Arr` / `ArrChoice` extensions: [[PipeArrow]].
 * Reference SMC: modules/volga-core (vendored volga).
 */

package zio.pdf.pipe

inline def sep: Unit = ()

opaque type Pipe[-A, +B] = A => B

object Pipe {

  def apply[A, B](f: A => B): Pipe[A, B] = f

  inline def comp[A, B](inline f: A => B): Pipe[A, B] = apply(f)

  def lift[A, B](f: A => B): Pipe[A, B] = apply(f)

  def id[A]: Pipe[A, A] = apply(a => a)

  private def fn[A, B](p: Pipe[A, B]): A => B = p

  extension [A, B](p: Pipe[A, B]) {
    /** Sequential composition (volga `>>>`, FreeScan `>>>`). */
    infix def >>>[C](g: Pipe[B, C]): Pipe[A, C] = apply(a => fn(g)(fn(p)(a)))
    infix def <<<[C](g: Pipe[C, A]): Pipe[C, B] = g >>> p

    /** Tensor / parallel on pairs (volga `><`, FreeScan `***`). */
    infix def ><[C, D](g: Pipe[C, D]): Pipe[(A, C), (B, D)] = par(p, g)
    infix def ***[C, D](g: Pipe[C, D]): Pipe[(A, C), (B, D)] = p >< g

    /** Cartesian fan-out (volga `<>`, FreeScan `&&&`). */
    infix def <>[C](g: Pipe[A, C]): Pipe[A, (B, C)] = fanOut(p, g)
    infix def &&&[C](g: Pipe[A, C]): Pipe[A, (B, C)] = p <> g

    def run(a: A): B = fn(p)(a)
  }

  def par[A, B, C, D](fa: Pipe[A, B], fb: Pipe[C, D]): Pipe[(A, C), (B, D)] =
    apply { case (a, c) => (fn(fa)(a), fn(fb)(c)) }

  def fanOut[A, B, C](f: Pipe[A, B], g: Pipe[A, C]): Pipe[A, (B, C)] =
    apply(a => (fn(f)(a), fn(g)(a)))

  /** Project the first component of a pair (volga `proj1`). */
  def proj1[A, B]: Pipe[(A, B), A] = apply(_._1)

  /** Project the second component of a pair (volga `proj2`). */
  def proj2[A, B]: Pipe[(A, B), B] = apply(_._2)

  /** Tuple first projection — prefer [[proj1]]; use [[PipeArrow.first]] for morphism threading. */
  def first[A, B]: Pipe[(A, B), A] = proj1

  /** Tuple second projection — prefer [[proj2]]; use [[PipeArrow.second]] for morphism threading. */
  def second[A, B]: Pipe[(A, B), B] = proj2

  def injectLeft[A, B]: Pipe[A, Either[A, B]]  = Pipe(Left(_))
  def injectRight[A, B]: Pipe[B, Either[A, B]] = Pipe(Right(_))

  def sum[A, B, C](left: Pipe[A, C], right: Pipe[B, C]): Pipe[Either[A, B], C] =
    apply(_.fold(left.run, right.run))
}
