package io.github.adrielc.kyopdf.pipeline

import PdfPipeline.*

/** Immutable typed PDF arrows. Construction describes a program; it never runs it. */
sealed abstract class PdfArrow[A, B] private[PdfArrow] (
    val domain: PdfArrow.Ports[A], val codomain: PdfArrow.Ports[B]):
  import PdfArrow.*
  final infix def >>>[C](next: PdfArrow[B, C]): PdfArrow[A, C] = Sequential(this, next)
  final infix def <<<[Z](previous: PdfArrow[Z, A]): PdfArrow[Z, B] = previous >>> this
  final infix def ><[C, D](other: PdfArrow[C, D]): PdfArrow[(A, C), (B, D)] = Parallel(this, other)
  final infix def ***[C, D](other: PdfArrow[C, D]): PdfArrow[(A, C), (B, D)] = this >< other
  final infix def *#*[C, D](other: PdfArrow[C, D]): PdfArrow[(A, C), (B, D)] = this >< other
  final infix def &&&[C](other: PdfArrow[A, C]): PdfArrow[A, (B, C)] = duplicate(domain) >>> (this >< other)
  final def first[C](using ports: Ports[C]): PdfArrow[(A, C), (B, C)] = this >< id[C]
  final def second[C](using ports: Ports[C]): PdfArrow[(C, A), (C, B)] = id[C] >< this
  /** Optional graph label; repeated uses still receive distinct, deterministic IDs. */
  final def named(name: String): PdfArrow[A, B] = Named(this, name)

object PdfArrow:
  sealed trait Document
  sealed trait File
  sealed trait Receipt

  /** Closed evidence for scalar ports, the tensor unit, and products of ports. */
  sealed abstract class Ports[A] private[PdfArrow] (val kinds: Vector[Kind])
  sealed abstract class Scalar[A] private[PdfArrow] (kind: Kind) extends Ports[A](Vector(kind))
  object Ports:
    given document: Scalar[Document] = new Scalar[Document](Kind.Document) {}
    given file: Scalar[File] = new Scalar[File](Kind.File) {}
    given receipt: Scalar[Receipt] = new Scalar[Receipt](Kind.Receipt) {}
    given unit: Ports[Unit] = new Ports[Unit](Vector.empty) {}
    given pair[A, B](using a: Ports[A], b: Ports[B]): Ports[(A, B)] = product(a, b)
  private def product[A, B](a: Ports[A], b: Ports[B]): Ports[(A, B)] = new Ports[(A, B)](a.kinds ++ b.kinds) {}

  private final case class Wiring[A, B](in: Ports[A], out: Ports[B], order: Vector[Int]) extends PdfArrow[A, B](in, out)
  private final case class Primitive[A, B](in: Ports[A], out: Ports[B], op: Op, label: String) extends PdfArrow[A, B](in, out)
  private final case class Sequential[A, B, C](left: PdfArrow[A, B], right: PdfArrow[B, C]) extends PdfArrow[A, C](left.domain, right.codomain)
  private final case class Parallel[A, B, C, D](left: PdfArrow[A, B], right: PdfArrow[C, D])
      extends PdfArrow[(A, C), (B, D)](product(left.domain, right.domain), product(left.codomain, right.codomain))
  private final case class Named[A, B](arrow: PdfArrow[A, B], label: String) extends PdfArrow[A, B](arrow.domain, arrow.codomain)
  private final case class Branch[A, B](condition: Condition[A], yes: PdfArrow[A, B], no: PdfArrow[A, B], unknown: PdfArrow[A, B])
      extends PdfArrow[A, B](yes.domain, yes.codomain)
  private final case class Reject[A, B](in: Ports[A], out: Ports[B], message: String) extends PdfArrow[A, B](in, out)
  private final case class Projection[A, B](in: Ports[A], out: Ports[B], indices: Vector[Int]) extends PdfArrow[A, B](in, out)

  final class Condition[A] private[PdfArrow] (private[PdfArrow] val source: PdfArrow[A, ?], private[PdfArrow] val test: Predicate):
    def on[Z](before: PdfArrow[Z, A]): Condition[Z] = new Condition(before >>> source, test)

  def pages(min: Int, max: Int): Condition[Document] = new Condition(id[Document], Predicate.PageCount(min, max))
  val declares3b: Condition[Document] = new Condition(id[Document], Predicate.Declares3b)
  def verified(profile: String): Condition[Receipt] = new Condition(id[Receipt], Predicate.Validation(profile))
  def choose[A, B: Scalar](condition: Condition[A])(yes: PdfArrow[A, B], no: PdfArrow[A, B], unknown: PdfArrow[A, B]): PdfArrow[A, B] =
    Branch(condition, yes, no, unknown)

  def id[A](using a: Ports[A]): PdfArrow[A, A] = Wiring(a, a, a.kinds.indices.toVector)
  def swap[A, B](using a: Ports[A], b: Ports[B]): PdfArrow[(A, B), (B, A)] =
    Wiring(product(a, b), product(b, a), (a.kinds.length until a.kinds.length + b.kinds.length).toVector ++ a.kinds.indices)
  def assocLeft[A, B, C](using a: Ports[A], b: Ports[B], c: Ports[C]): PdfArrow[(A, (B, C)), ((A, B), C)] =
    val in = product(a, product(b, c))
    Wiring(in, product(product(a, b), c), in.kinds.indices.toVector)
  def assocRight[A, B, C](using a: Ports[A], b: Ports[B], c: Ports[C]): PdfArrow[((A, B), C), (A, (B, C))] =
    val in = product(product(a, b), c)
    Wiring(in, product(a, product(b, c)), in.kinds.indices.toVector)
  def leftUnit[A](using a: Ports[A]): PdfArrow[(Unit, A), A] = Wiring(product(summon[Ports[Unit]], a), a, a.kinds.indices.toVector)
  def spawnLeft[A](using a: Ports[A]): PdfArrow[A, (Unit, A)] = Wiring(a, product(summon[Ports[Unit]], a), a.kinds.indices.toVector)
  def fork[A](using a: Ports[A]): PdfArrow[A, (A, A)] = duplicate(a)
  private def duplicate[A](a: Ports[A]): PdfArrow[A, (A, A)] = Wiring(a, product(a, a), a.kinds.indices.toVector ++ a.kinds.indices)

  /** Explicit projections evaluate both inputs; this is not implicit wire deletion. */
  def keepLeft[A, B](using a: Scalar[A], b: Ports[B]): PdfArrow[(A, B), A] = Projection(product(a, b), a, a.kinds.indices.toVector)
  def keepRight[A, B](using a: Ports[A], b: Scalar[B]): PdfArrow[(A, B), B] =
    Projection(product(a, b), b, (a.kinds.length until a.kinds.length + b.kinds.length).toVector)
  def stop[A, B](message: String)(using a: Ports[A], b: Scalar[B]): PdfArrow[A, B] = Reject(a, b, message)

  private def step[A, B](op: Op, label: String)(using a: Ports[A], b: Ports[B]): PdfArrow[A, B] = Primitive(a, b, op, label)
  def input(slot: String): PdfArrow[Unit, Document] = step(Op.Input(slot), slot)
  def select(pages: Int*): PdfArrow[Document, Document] = step(Op.Select(pages.toVector), "cut")
  def stamp(text: String, placement: String = "top"): PdfArrow[Document, Document] = step(Op.Stamp(text, placement), "stamp")
  def number(start: Int = 1, style: String = "page", prefix: String = ""): PdfArrow[Document, Document] = step(Op.NumberPages(start, style, prefix), "numbers")
  val write: PdfArrow[Document, File] = step(Op.Write, "pdf")
  def validate(profiles: String*): PdfArrow[File, Receipt] = step(Op.Validate(profiles.toVector), "checks")
  def assemble(catalog: Int = 0): PdfArrow[(Document, Document), Document] = step(Op.Assemble(catalog), "assembled")
  def assemble3(catalog: Int = 0): PdfArrow[((Document, Document), Document), Document] = step(Op.Assemble(catalog), "assembled")
  def assemble4(catalog: Int = 0): PdfArrow[(((Document, Document), Document), Document), Document] = step(Op.Assemble(catalog), "assembled")

  /** Compile a closed arrow into portable data. No operation IDs are needed to compose arrows. */
  def recipe[B](name: String, arrow: PdfArrow[Unit, B]): Either[Vector[Issue], Recipe] =
    val nodes = scala.collection.mutable.ArrayBuffer.empty[Node]
    val allocated = scala.collection.mutable.Set.empty[String]
    def add(op: Op, inputs: Vector[String], preferred: String): String =
      var id = preferred
      var suffix = 2
      while allocated(id) do
        id = s"${preferred.take(43)}-$suffix"
        suffix += 1
      allocated += id
      nodes += Node(id, op, inputs)
      id
    def walk(value: PdfArrow[?, ?], inputs: Vector[String], label: Option[String] = None): Vector[String] = value match
      case Wiring(_, _, order) => order.map(inputs)
      case Primitive(_, _, op, name) => Vector(add(op, inputs, label.getOrElse(name)))
      case Sequential(first, second) => walk(second, walk(first, inputs), label)
      case Parallel(left, right) =>
        walk(left, inputs.take(left.domain.kinds.length), label.map(_ + "-left")) ++
          walk(right, inputs.drop(left.domain.kinds.length), label.map(_ + "-right"))
      case Named(arrow, name) => walk(arrow, inputs, Some(name))
      case branch: Branch[?, ?] =>
        val condition = walk(branch.condition.source, inputs).head
        val arms = Vector(branch.yes, branch.no, branch.unknown).map(walk(_, inputs))
        branch.codomain.kinds.zipWithIndex.map { (kind, index) =>
          add(Op.Choose(branch.condition.test, kind), Vector(condition) ++ arms.map(_(index)), label.getOrElse("route"))
        }
      case Reject(in, out, message) => out.kinds.map { kind =>
        val op = if inputs.isEmpty then Op.Stop(message, kind) else Op.StopAfter(message, in.kinds, kind)
        add(op, inputs, label.getOrElse("stop"))
      }
      case Projection(in, _, indices) => indices.map(index => add(Op.Keep(index, in.kinds), inputs, label.getOrElse("keep")))
    val roots = walk(arrow, Vector.empty)
    val seenOutputs = scala.collection.mutable.Set.empty[String]
    val outputs = roots.zip(arrow.codomain.kinds).map { (id, kind) =>
      if seenOutputs.add(id) then id else add(Op.Keep(0, Vector(kind)), Vector(id), "output")
    }
    val version = if nodes.exists(n => n.op.isInstanceOf[Op.Keep] || n.op.isInstanceOf[Op.StopAfter]) then 3
      else if nodes.exists(n => n.op.isInstanceOf[Op.Choose] || n.op.isInstanceOf[Op.Stop]) then 2 else 1
    PdfPipeline.check(Recipe(version, name, nodes.toVector, outputs)).map(_.recipe)
