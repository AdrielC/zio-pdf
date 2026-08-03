package zio.blocks.pure

import scala.collection.BuildFrom
import scala.reflect.ClassTag

/** Evidence that `E` may fail (not `Nothing`). */
trait CanFail[-E]
object CanFail {
  implicit def canFail[E]: CanFail[E] = new CanFail[E] {}
}

/** Service locator tag for [[Env]]. */
trait Tag[A] {
  def clazz: Class[_]
}
object Tag {
  implicit def fromClassTag[A](implicit ct: ClassTag[A]): Tag[A] =
    new Tag[A] { val clazz: Class[_] = ct.runtimeClass }
}

/** Zip two values into a product (replaces zio-prelude `Zippable`). */
trait Zippable[-A, -B] {
  type Out
  def zip(left: A, right: B): Out
}
object Zippable {
  type Aux[A, B, C] = Zippable[A, B] { type Out = C }

  given zippable2[A, B]: Zippable.Aux[A, B, (A, B)] = new Zippable[A, B] {
    type Out = (A, B)
    def zip(left: A, right: B): (A, B) = (left, right)
  }

  given zippable3[A, B, C]: Zippable.Aux[(A, B), C, (A, B, C)] = new Zippable[(A, B), C] {
    type Out = (A, B, C)
    def zip(left: (A, B), right: C): (A, B, C) = (left._1, left._2, right)
  }
}

/** Validation result with an accumulated log (replaces `ZValidation`). */
sealed trait Validation[+W, +E, +A]
object Validation {
  final case class Success[W, A](log: zio.blocks.chunk.Chunk[W], value: A) extends Validation[W, Nothing, A]
  final case class Failure[W, E](log: zio.blocks.chunk.Chunk[W], error: E)  extends Validation[W, E, Nothing]
}

trait Covariant[F[+_]] {
  def map[A, B](f: A => B): F[A] => F[B]
}

trait IdentityBoth[F[+_]] extends Covariant[F] {
  def any: F[Any]
  def both[A, B](fa: => F[A], fb: => F[B]): F[(A, B)]
}

trait CovariantIdentityBoth[F[+_]] extends IdentityBoth[F] {
  def collectM[A, B, Collection[+Element] <: Iterable[Element]](in: Collection[A])(
    f: A => F[Option[B]]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]]): F[Collection[B]]

  def forEach[A, B, Collection[+Element] <: Iterable[Element]](in: Collection[A])(
    f: A => F[B]
  )(implicit bf: BuildFrom[Collection[A], B, Collection[B]]): F[Collection[B]]

  def forEach_[A](in: Iterable[A])(f: A => F[Any]): F[Unit]
}

trait IdentityFlatten[F[+_]] extends IdentityBoth[F] {
  def flatten[A](ffa: F[F[A]]): F[A]
}
