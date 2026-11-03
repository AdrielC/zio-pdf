package io.github.adrielc.kyopdf.pipeline

import PdfPipeline.*

/** Typed Scala authoring; the result is the same closed data graph as saved JSON. */
final class RecipeBuilder:
  sealed trait DocumentPort
  sealed trait FilePort
  sealed trait ReceiptPort
  final class Wire[A] private[RecipeBuilder] (private[RecipeBuilder] val id: String)
  private var nodes = Vector.empty[Node]
  private def node[A](op: Op, inputs: Vector[String]): Wire[A] =
    val id = s"step${nodes.length + 1}"
    nodes :+= Node(id, op, inputs)
    new Wire[A](id)
  def input(slot: String): Wire[DocumentPort] = node(Op.Input(slot), Vector.empty)
  def select(input: Wire[DocumentPort], pages: Int*): Wire[DocumentPort] = node(Op.Select(pages.toVector), Vector(input.id))
  def assemble(inputs: Vector[Wire[DocumentPort]], catalog: Int = 0): Wire[DocumentPort] = node(Op.Assemble(catalog), inputs.map(_.id))
  def stamp(input: Wire[DocumentPort], text: String, placement: String = "top"): Wire[DocumentPort] = node(Op.Stamp(text, placement), Vector(input.id))
  def number(input: Wire[DocumentPort], start: Int = 1, style: String = "page", prefix: String = ""): Wire[DocumentPort] =
    node(Op.NumberPages(start, style, prefix), Vector(input.id))
  def write(input: Wire[DocumentPort]): Wire[FilePort] = node(Op.Write, Vector(input.id))
  def validate(input: Wire[FilePort], profiles: String*): Wire[ReceiptPort] = node(Op.Validate(profiles.toVector), Vector(input.id))
  def recipe(name: String, file: Wire[FilePort], receipts: Wire[ReceiptPort]*): Recipe =
    Recipe(1, name, nodes, Vector(file.id) ++ receipts.map(_.id))
