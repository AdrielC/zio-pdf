package zio.pdf.pipe.functors

/** Minimal monad — enough for [[State]]; full volga `Monad` lives in `modules/volga`. */
trait Monad[F[_]]:
  def pure[A](x: A): F[A]
  extension [A](fa: F[A]) def flatMap[B](f: A => F[B]): F[B]
  extension [A](fa: F[A])
    infix def >>[B](fb: => F[B]): F[B] = fa.flatMap(_ => fb)
