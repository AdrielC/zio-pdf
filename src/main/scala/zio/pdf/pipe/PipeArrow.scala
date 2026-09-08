/*
 * volga / cats `Arr` + `ArrChoice` operators over [[Pipe]].
 *
 * Projection helpers live on [[Pipe.proj1]] / [[Pipe.proj2]] (volga `proj1`/`proj2`).
 * Morphism threading uses extension methods here (`first`, `second`, `left`, `right`).
 *
 * Reference: modules/volga-core/.../old/core/.../Arr.scala
 */

package zio.pdf.pipe

object PipeArrow {

  /** Route by predicate: `test(p)(yes)(no)`. */
  def test[A, B](p: A => Boolean)(yes: Pipe[A, B])(no: Pipe[A, B]): Pipe[A, B] =
    Pipe(a => if p(a) then yes.run(a) else no.run(a))

  /** Collapse `Either[A, A]` to `A`. */
  def merge[A]: Pipe[Either[A, A], A] =
    Pipe(_.merge)

  /** `a => (a, a)` — volga copy / diagonal. */
  def diag[A]: Pipe[A, (A, A)] =
    Pipe(a => (a, a))

  /** Swap tuple components. */
  def swap[A, B]: Pipe[(A, B), (B, A)] =
    Pipe { case (a, b) => (b, a) }

  /** Swap Either branches. */
  def mirror[A, B]: Pipe[Either[A, B], Either[B, A]] =
    Pipe {
      case Left(a)  => Right(a)
      case Right(b) => Left(b)
    }

  extension [A, B](self: Pipe[A, B]) {

    /** Post-compose a pure function (volga `rmap`). */
    def map[C](f: B => C): Pipe[A, C] =
      self >>> Pipe(f)

    /** Pre-compose a pure function (volga `lmap`). */
    def contramap[C](f: C => A): Pipe[C, B] =
      Pipe(f) >>> self

    /** Profunctor `dimap`. */
    def dimap[C, D](pre: C => A, post: B => D): Pipe[C, D] =
      contramap(pre).map(post)

    /** Run on the first component of a pair (volga `Arr.first` — not [[Pipe.proj1]]). */
    def first[C]: Pipe[(A, C), (B, C)] =
      Pipe.par(self, Pipe.id)

    /** Run on the second component of a pair (volga `Arr.second`). */
    def second[C]: Pipe[(C, A), (C, B)] =
      Pipe.par(Pipe.id, self)

    /** Route `Left` through `self`, pass `Right` unchanged (volga `ArrChoice.left`). */
    def left[C]: Pipe[Either[A, C], Either[B, C]] =
      choose(self, Pipe.id)

    /** Route `Right` through `self`, pass `Left` unchanged (volga `ArrChoice.right`). */
    def right[C]: Pipe[Either[C, A], Either[C, B]] =
      choose(Pipe.id, self)

    /** Routed merge to a single output (volga `ArrChoice.choice` / Haskell `|||`). */
    infix def |||[C](other: Pipe[C, B]): Pipe[Either[A, C], B] =
      Pipe.sum(self, other)

    /** Disjoint parallel — preserve the Either shape (Haskell `+++`). */
    infix def +++[C, D](other: Pipe[C, D]): Pipe[Either[A, C], Either[B, D]] =
      Pipe {
        case Left(a)  => Left(self.run(a))
        case Right(c) => Right(other.run(c))
      }

    /** Fan-out into a tuple (volga `product` / `<>`). */
    infix def &&&[C](other: Pipe[A, C]): Pipe[A, (B, C)] =
      Pipe.fanOut(self, other)

    /** Tensor on independent inputs (volga `split` / `***`). */
    infix def ***[C, D](other: Pipe[C, D]): Pipe[(A, C), (B, D)] =
      Pipe.par(self, other)
  }

  /** Route each branch independently (volga `ArrChoice.choose`). */
  def choose[A, B, C, D](left: Pipe[A, C], right: Pipe[B, D]): Pipe[Either[A, B], Either[C, D]] =
    Pipe {
      case Left(a)  => Left(left.run(a))
      case Right(b) => Right(right.run(b))
    }
}
