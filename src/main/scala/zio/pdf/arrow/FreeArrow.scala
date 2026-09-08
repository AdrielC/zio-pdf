package zio.pdf.arrow

import ArrowObjects.*
import ArrowObjects.given
import BiFunctionK.~~>

/**
 * Free arrow AST with `foldMap` / `compile` — volga `FreeCat` shape, interpreted
 * over plain Scala object types via [[ArrowObjects]].
 */
sealed abstract class FreeArrow[Flow[_, _], In, Out] {
  self =>

  type Monoidal >: ScanGraph.Empty.type

  def foldMap[G[_, _]](fg: Flow ~~> G)(using G: Category[G]): G[In, Out]

  def fold[FF[a, b] >: Flow[a, b]](using FF: Category[FF]): FF[In, Out] =
    foldMap(new BiFunctionK[Flow, FF] {
      def apply[A, B](fa: Flow[A, B]): FF[A, B] = fa
    })

  final def compile[G[_, _]](fg: Flow ~~> G)(using G: Category[G]): G[In, Out] =
    foldMap(fg)
}

object FreeArrow {

  type Aux[Flow[_, _], In, Out, M0] = FreeArrow[Flow, In, Out] { type Monoidal = M0 }

  type ~~>[F[_, _], G[_, _]] = BiFunctionK[F, G]
  type ~| [F[_, _], M]       = [A, B] => F[A, B] => M

  def ident[Flow[_, _], A]: FreeArrow[Flow, A, A] = Id()

  def embed[Flow[_, _], A, B](f: Flow[A, B]): FreeArrow[Flow, A, B] = Embed(f)

  def sequential[Flow[_, _], A, M, B](
      left:  FreeArrow[Flow, A, M],
      right: FreeArrow[Flow, M, B]
  ): FreeArrow[Flow, A, B] = Seq(left, right)

  def parallel[Flow[_, _], A, B, C, D](
      left:  FreeArrow[Flow, A, B],
      right: FreeArrow[Flow, C, D]
  ): FreeArrow[Flow, (A, C), (B, D)] = Par(left, right)

  private final case class Id[Flow[_, _], A]() extends FreeArrow[Flow, A, A] {
    type Monoidal = ScanGraph.Empty.type
    def foldMap[G[_, _]](fg: Flow ~~> G)(using G: Category[G]): G[A, A] = G.id
  }

  private final case class Embed[Flow[_, _], A, B](f: Flow[A, B]) extends FreeArrow[Flow, A, B] {
    type Monoidal = ScanGraph.Empty.type
    def foldMap[G[_, _]](fg: Flow ~~> G)(using G: Category[G]): G[A, B] = fg(f)
  }

  private final case class Seq[Flow[_, _], A, M, B](
      left:  FreeArrow[Flow, A, M],
      right: FreeArrow[Flow, M, B]
  ) extends FreeArrow[Flow, A, B] {
    type Monoidal = ScanGraph.Empty.type
    def foldMap[G[_, _]](fg: Flow ~~> G)(using G: Category[G]): G[A, B] =
      G.compose(right.foldMap(fg), left.foldMap(fg))
  }

  private final case class Par[Flow[_, _], A: Ob, B: Ob, C: Ob, D: Ob](
      left:  FreeArrow[Flow, A, B],
      right: FreeArrow[Flow, C, D]
  ) extends FreeArrow[Flow, (A, C), (B, D)] {
    type Monoidal = ScanGraph.Empty.type
    def foldMap[G[_, _]](fg: Flow ~~> G)(using G: Category[G]): G[(A, C), (B, D)] =
      G.split(left.foldMap(fg), right.foldMap(fg))
  }

  def fanout[Flow[_, _], A: Ob, B: Ob, C: Ob](
      left:  FreeArrow[Flow, A, B],
      right: FreeArrow[Flow, A, C]
  )(using F: Category[Flow]): FreeArrow[Flow, A, Prod[B, C]] =
    sequential(
      embed(F.fanout(F.id[A], F.id[A])),
      parallel(left, right)
    )

  extension [Flow[_, _], A, B](self: FreeArrow[Flow, A, B]) {
    infix def >>>[C](right: FreeArrow[Flow, B, C]): FreeArrow[Flow, A, C] =
      sequential(self, right)

    infix def ***[C, D](right: FreeArrow[Flow, C, D]): FreeArrow[Flow, (A, C), (B, D)] =
      parallel(self, right)

    infix def &&&[C](right: FreeArrow[Flow, A, C])(using F: Category[Flow]): FreeArrow[Flow, A, Prod[B, C]] =
      fanout(self, right)
  }
}
