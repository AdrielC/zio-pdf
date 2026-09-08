/*
 * [[CartesianCat]] / [[ApplyCat]] for [[Pipe]] — volga arrow kit over fused functions.
 */

package zio.pdf.pipe

import volga.*
import PipeObjects.U
import PipeObjects.monoidalObjects
import PipeObjects.scalaObjects

object PipeCat:

  given pipeCat: Cat[Pipe, U] with
    def identity[A: Ob]: Pipe[A, A] = Pipe(a => a)
    def compose[A: Ob, B: Ob, C: Ob](f: Pipe[B, C], g: Pipe[A, B]): Pipe[A, C] =
      g >>> f

  given pipeMonoidal: MonoidalCat[Pipe, U] with
    export pipeCat.{identity, compose}
    def tensor[A: Ob, B: Ob, C: Ob, D: Ob](f: Pipe[A, B], g: Pipe[C, D]): Pipe[(A, C), (B, D)] =
      Pipe.par(f, g)
    def associate[A: Ob, B: Ob, C: Ob]: (A, (B, C)) <--> ((A, B), C) =
      Iso(
        Pipe { case (a, (b, c)) => ((a, b), c) },
        Pipe { case ((a, b), c) => (a, (b, c)) }
      )
    def leftUnit[A: Ob]: (Unit, A) <--> A =
      Iso(Pipe { case (_, a) => a }, Pipe(a => ((), a)))
    def rightUnit[A: Ob]: (A, Unit) <--> A =
      Iso(Pipe { case (a, _) => a }, Pipe(a => (a, ())))

  given pipeSymmetric: SymmetricCat[Pipe, U] with
    export pipeMonoidal.{identity, compose, tensor, leftUnit}
    def braiding[A: Ob, B: Ob]: Pipe[(A, B), (B, A)] =
      Pipe { case (a, b) => (b, a) }
    def assocLeft[A: Ob, B: Ob, C: Ob]: Pipe[(A, (B, C)), ((A, B), C)] =
      Pipe { case (a, (b, c)) => ((a, b), c) }

  given pipeCartesian: CartesianCat[Pipe, U] with
    export pipeSymmetric.{identity, compose, tensor, assocLeft}
    def terminal[A: Ob]: Pipe[A, Unit] = Pipe(_ => ())
    def projectLeft[A: Ob, B: Ob]: Pipe[(A, B), A]  = Pipe.proj1
    def projectRight[A: Ob, B: Ob]: Pipe[(A, B), B] = Pipe.proj2
    def product[A: Ob, B: Ob, C: Ob](f: Pipe[A, B], g: Pipe[A, C]): Pipe[A, (B, C)] =
      Pipe.fanOut(f, g)

  given pipeApply: ApplyCat[Pipe, U] with
    export pipeSymmetric.{identity, compose, tensor, leftUnit, braiding, assocLeft}
    def lift[A, B](f: A => B): Pipe[A, B] = Pipe(f)
    def scalaUnit: Pipe[Unit, Unit]       = Pipe(u => u)
    def zip[A, B]: Pipe[(A, B), (A, B)]   = Pipe(p => p)

  /** Sum-side structure over `Either` / `Nothing` — volga `CocartesianCat`. */
  given pipeCocartesian: CocartesianCat[Pipe, U] with
    given zeroOb: Ob[O]                   = PipeObjects.ob
    given sumOb[A: Ob, B: Ob]: Ob[A + B] = PipeObjects.ob
    export pipeCat.{identity, compose}
    def initial[A: Ob]: Pipe[Nothing, A] =
      Pipe((n: Nothing) => n)
    def injectLeft[A: Ob, B: Ob]: Pipe[A, Either[A, B]]   = Pipe(Left(_))
    def injectRight[A: Ob, B: Ob]: Pipe[B, Either[A, B]]  = Pipe(Right(_))
    def sum[A: Ob, B: Ob, C: Ob](f: Pipe[A, C], g: Pipe[B, C]): Pipe[Either[A, B], C] =
      Pipe(_.fold(f.run, g.run))

  /** Cartesian × cocartesian distributive laws — volga `DistributiveCat`. */
  given pipeDistributive: DistributiveCat[Pipe, U] with
    given zeroOb: Ob[O]                   = PipeObjects.ob
    given sumOb[A: Ob, B: Ob]: Ob[A + B] = PipeObjects.ob
    export pipeCartesian.{
      identity,
      compose,
      tensor,
      assocLeft,
      terminal,
      projectLeft,
      projectRight,
      product
    }
    export pipeCocartesian.{initial, injectLeft, injectRight, sum}
    def distributeFrom[A: Ob, B: Ob, C: Ob]: Pipe[(A, Either[B, C]), Either[(A, B), (A, C)]] =
      Pipe {
        case (a, Left(b))  => Left((a, b))
        case (a, Right(c)) => Right((a, c))
      }
