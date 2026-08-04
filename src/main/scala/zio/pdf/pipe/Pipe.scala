/*
 * Fused pure morphisms. Operators mirror volga / FreeScan arrows:
 *   >>>  compose      >< / ***  tensor     <> / &&&  fanout
 *
 * Full [[CartesianCat]] instance: [[PipeCat.pipeCartesian]].
 * Reference SMC: modules/volga (git submodule).
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

  def first[A, B]: Pipe[(A, B), A]  = apply(_._1)
  def second[A, B]: Pipe[(A, B), B] = apply(_._2)
}
