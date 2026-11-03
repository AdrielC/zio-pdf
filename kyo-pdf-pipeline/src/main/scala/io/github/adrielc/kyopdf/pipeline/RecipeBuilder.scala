package io.github.adrielc.kyopdf.pipeline

import PdfPipeline.*

/** Typed Scala authoring; the result is the same closed data graph as saved JSON. */
final class RecipeBuilder:
  sealed trait DocumentPort
  sealed trait FilePort
  sealed trait ReceiptPort
  final class Wire[A] private[RecipeBuilder] (private[RecipeBuilder] val id: String, private[RecipeBuilder] val kind: Kind)
  private var nodes = Vector.empty[Node]
  private def node[A](op: Op, inputs: Vector[String]): Wire[A] =
    val id = s"step${nodes.length + 1}"
    nodes :+= Node(id, op, inputs)
    new Wire[A](id, op.output)
  def input(slot: String): Wire[DocumentPort] = node(Op.Input(slot), Vector.empty)
  def select(input: Wire[DocumentPort], pages: Int*): Wire[DocumentPort] = node(Op.Select(pages.toVector), Vector(input.id))
  def assemble(inputs: Vector[Wire[DocumentPort]], catalog: Int = 0): Wire[DocumentPort] = node(Op.Assemble(catalog), inputs.map(_.id))
  def stamp(input: Wire[DocumentPort], text: String, placement: String = "top"): Wire[DocumentPort] = node(Op.Stamp(text, placement), Vector(input.id))
  def number(input: Wire[DocumentPort], start: Int = 1, style: String = "page", prefix: String = ""): Wire[DocumentPort] =
    node(Op.NumberPages(start, style, prefix), Vector(input.id))
  def write(input: Wire[DocumentPort]): Wire[FilePort] = node(Op.Write, Vector(input.id))
  def validate(input: Wire[FilePort], profiles: String*): Wire[ReceiptPort] = node(Op.Validate(profiles.toVector), Vector(input.id))
  def stopDocument(message: String): Wire[DocumentPort] = node(Op.Stop(message, Kind.Document), Vector.empty)
  def stopFile(message: String): Wire[FilePort] = node(Op.Stop(message, Kind.File), Vector.empty)
  def stopReceipt(message: String): Wire[ReceiptPort] = node(Op.Stop(message, Kind.Receipt), Vector.empty)
  def choosePages[A](source: Wire[DocumentPort], min: Int, max: Int)(yes: Wire[A], no: Wire[A], unknown: Wire[A]): Wire[A] =
    node(Op.Choose(Predicate.PageCount(min, max), yes.kind), Vector(source.id, yes.id, no.id, unknown.id))
  def chooseDeclared3b[A](source: Wire[DocumentPort])(yes: Wire[A], no: Wire[A], unknown: Wire[A]): Wire[A] =
    node(Op.Choose(Predicate.Declares3b, yes.kind), Vector(source.id, yes.id, no.id, unknown.id))
  def chooseValidation[A](source: Wire[ReceiptPort], profile: String)(passed: Wire[A], failed: Wire[A], unable: Wire[A]): Wire[A] =
    node(Op.Choose(Predicate.Validation(profile), passed.kind), Vector(source.id, passed.id, failed.id, unable.id))
  private def version: Int = if nodes.exists(n => n.op.isInstanceOf[Op.Choose] || n.op.isInstanceOf[Op.Stop]) then 2 else 1
  def recipe(name: String, files: Vector[Wire[FilePort]], receipts: Vector[Wire[ReceiptPort]]): Recipe =
    Recipe(version, name, nodes, files.map(_.id) ++ receipts.map(_.id))
  def recipe(name: String, file: Wire[FilePort], receipts: Wire[ReceiptPort]*): Recipe =
    Recipe(version, name, nodes, Vector(file.id) ++ receipts.map(_.id))
