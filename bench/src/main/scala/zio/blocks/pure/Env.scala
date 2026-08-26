package zio.pdf.bench.pure

import scala.collection.immutable.HashMap

/** Heterogeneous service environment (zio-blocks replacement for `ZEnvironment`). */
final class Env private (private val services: HashMap[Class[?], Any]) extends AnyVal {

  def get[A](implicit tag: Tag[A]): A =
    services(tag.clazz).asInstanceOf[A]

  def add[A](service: A)(implicit tag: Tag[A]): Env =
    new Env(services.updated(tag.clazz, service))

  def ++(that: Env): Env =
    new Env(services ++ that.services)
}

object Env {
  val empty: Env = new Env(HashMap.empty)

  def apply[A](service: A)(implicit tag: Tag[A]): Env =
    empty.add(service)
}
