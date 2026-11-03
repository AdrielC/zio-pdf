package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.pipeline.PdfPipeline.*
import volga.SymmetricCat
import volga.syntax.smc
import volga.free.{FreeProp, Nat, PropOb}

/** Real Volga SMC authoring compiled to the portable recipe, including fork/join.
  * Volga checks port counts; PdfPipeline.check also checks payload kinds/settings.
  */
object PipelineWiring:
  final case class Step[I, O](id: String, op: Option[Op])
  type Diagram[I, O] = FreeProp[Step, I, O]
  given SymmetricCat[Diagram, PropOb] = FreeProp.propCat[Step]
  val syntax = smc.syntax[Diagram, PropOb, Nat.Plus]
  def source(id: String, slot: String): Diagram[Nat.`0`, Nat.`1`] = FreeProp.Embed(Step(id, Some(Op.Input(slot))))
  def step(id: String, op: Op): Diagram[Nat.`1`, Nat.`1`] = FreeProp.Embed(Step(id, Some(op)))
  def join(id: String, catalog: Int = 0): Diagram[Nat.`2`, Nat.`1`] = FreeProp.Embed(Step(id, Some(Op.Assemble(catalog))))
  def choose(id: String, test: Predicate, kind: Kind = Kind.Document): Diagram[Nat.OfInt[4], Nat.`1`] =
    FreeProp.Embed(Step(id, Some(Op.Choose(test, kind))))
  def stop(id: String, message: String, kind: Kind = Kind.Document): Diagram[Nat.`0`, Nat.`1`] =
    FreeProp.Embed(Step(id, Some(Op.Stop(message, kind))))
  val fork: Diagram[Nat.`1`, Nat.`2`] = FreeProp.Embed(Step("fork", None))

  def recipe[N: Nat](name: String, diagram: Diagram[Nat.`0`, N]): Either[Vector[Issue], Recipe] =
    val nodes = Vector.newBuilder[Node]
    def count(n: Nat[?]): Int = n match
      case Nat.Zero() => 0
      case Nat.Succ(previous) => 1 + count(previous)
    def walk[A: Nat, B: Nat](value: Diagram[A, B], inputs: Vector[String]): Vector[String] = value match
      case FreeProp.Id() => inputs
      case FreeProp.Embed(step) => step.op match
        case None => Vector(inputs.head, inputs.head)
        case Some(op) =>
          nodes += Node(step.id, op, inputs)
          Vector(step.id)
      case FreeProp.AndThen(first, second, middle) =>
        walk(second, walk(first, inputs)(using summon[Nat[A]], middle))(using middle, summon[Nat[B]])
      case FreeProp.Par(left, right, a, b, c, d) =>
        walk(left, inputs.take(count(a)))(using a, b) ++ walk(right, inputs.drop(count(a)))(using c, d)
      case FreeProp.Swap(a, _) => inputs.drop(count(a)) ++ inputs.take(count(a))
    val outputs = walk(diagram, Vector.empty)
    val items = nodes.result()
    val version = if items.exists(n => n.op.isInstanceOf[Op.Choose] || n.op.isInstanceOf[Op.Stop]) then 2 else 1
    val result = Recipe(version, name, items, outputs)
    check(result).map(_.recipe)

  def example: Recipe =
    val read = source("body", "body")
    val select = step("cut", Op.Select(Vector(4, 1, 3)))
    val stamp = step("stamp", Op.Stamp("DRAFT"))
    val number = step("numbers", Op.NumberPages(1, "bates", "DOC-"))
    val write = step("pdf", Op.Write)
    val validate = step("checks", Op.Validate(Vector("3b", "2b", "3u")))
    val graph = syntax.of0:
      val body = read()
      val cut = select(body)
      val marked = stamp(cut)
      val numbered = number(marked)
      val bytes = write(numbered)
      val (download, check) = fork(bytes)
      val receipt = validate(check)
      (download, receipt)
    recipe("Marked packet", graph).fold(errors => throw IllegalStateException(errors.mkString(", ")), identity)

  def conditionalExample: Recipe =
    val read = source("body", "body")
    val short = step("short", Op.Stamp("SHORT PACKET"))
    val long = step("long", Op.Stamp("LONG PACKET"))
    val unknown = stop("unknown", "The document page count could not be determined")
    val route = choose("route", Predicate.PageCount(1, 3))
    val write = step("pdf", Op.Write)
    val checks = step("checks", Op.Validate(Vector("3b")))
    val graph = syntax.of0:
      val body = read()
      val (condition, branches) = fork(body)
      val (small, large) = fork(branches)
      val shortPacket = short(small)
      val longPacket = long(large)
      val unavailable = unknown()
      val selected = route(condition, shortPacket, longPacket, unavailable)
      val bytes = write(selected)
      val (download, validate) = fork(bytes)
      val receipt = checks(validate)
      (download, receipt)
    recipe("Route by page count", graph).fold(errors => throw IllegalStateException(errors.mkString(", ")), identity)
