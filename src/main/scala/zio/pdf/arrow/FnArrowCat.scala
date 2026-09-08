package zio.pdf.arrow

import ArrowObjects.*

/** [[Category]] instance for [[FnArrow]] using path-dependent products / coproducts. */
object FnArrowCat {

  given fnCompose: Compose[FnArrow] with {
    def id[A]: FnArrow[A, A] = FnArrow.id
    def compose[A, B, C](f: FnArrow[B, C], g: FnArrow[A, B]): FnArrow[A, C] =
      g >>> f
  }

  given fnCategory: Category[FnArrow] with {
    export fnCompose.{id, compose}

    def split[A: Ob, B: Ob, C: Ob, D: Ob](
        f: FnArrow[A, B],
        g: FnArrow[C, D]
    ): FnArrow[Prod[A, C], Prod[B, D]] =
      f *** g

    def fanout[A: Ob, B: Ob, C: Ob](
        f: FnArrow[A, B],
        g: FnArrow[A, C]
    ): FnArrow[A, Prod[B, C]] =
      f &&& g

    def merge[A: Ob, B: Ob, C: Ob](
        f: FnArrow[A, C],
        g: FnArrow[B, C]
    ): FnArrow[Sum[A, B], C] =
      FnArrow {
        case Left(a)  => f.run(a)
        case Right(b) => g.run(b)
      }

    def choose[A: Ob, B: Ob, C: Ob, D: Ob](
        f: FnArrow[A, C],
        g: FnArrow[B, D]
    ): FnArrow[Sum[A, B], Sum[C, D]] =
      FnArrow {
        case Left(a)  => Left(f.run(a))
        case Right(b) => Right(g.run(b))
      }

    def injectLeft[A: Ob, B: Ob]: FnArrow[A, Sum[A, B]] =
      FnArrow(Left(_))

    def injectRight[A: Ob, B: Ob]: FnArrow[B, Sum[A, B]] =
      FnArrow(Right(_))
  }
}
