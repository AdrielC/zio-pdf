/*
 * Compile-time US-ASCII interpolators for keyword / magic-byte constants.
 *
 *   ascii"xref"        // ByteVector, baked at compile time
 *   asciiBytes"Length" // Array[Byte]
 *
 * Interpolated arguments and non-ASCII code points fail compilation.
 */

package zio.pdf

import scala.quoted.*

import _root_.scodec.bits.ByteVector

extension (inline sc: StringContext)
  inline def ascii(inline args: Any*): ByteVector =
    ${ AsciiMacros.byteVector('sc, 'args) }

  inline def asciiBytes(inline args: Any*): Array[Byte] =
    ${ AsciiMacros.array('sc, 'args) }

private object AsciiMacros {

  def byteVector(sc: Expr[StringContext], args: Expr[Seq[Any]])(using Quotes): Expr[ByteVector] =
    val bytes = literal(sc, args)
    if bytes.isEmpty then '{ ByteVector.empty }
    else '{ ByteVector.view(${ arrayExpr(bytes) }) }

  def array(sc: Expr[StringContext], args: Expr[Seq[Any]])(using Quotes): Expr[Array[Byte]] =
    arrayExpr(literal(sc, args))

  private def literal(sc: Expr[StringContext], args: Expr[Seq[Any]])(using Quotes): Array[Byte] =
    import quotes.reflect.*
    args match
      case Varargs(Seq()) => ()
      case _ =>
        report.errorAndAbort("ascii\"...\" does not interpolate arguments; use a literal only")
    val parts = sc.valueOrAbort.parts
    if parts.size != 1 then
      report.errorAndAbort("ascii\"...\" does not interpolate arguments; use a literal only")
    val text = parts.head
    val bad  = text.filter(_.toInt > 127)
    if bad.nonEmpty then
      val shown = bad.map(c => f"U+${c.toInt}%04X").mkString(", ")
      report.errorAndAbort(s"ascii\"...\" is US-ASCII only; non-ASCII: $shown")
    text.getBytes(java.nio.charset.StandardCharsets.US_ASCII)

  private def arrayExpr(bytes: Array[Byte])(using Quotes): Expr[Array[Byte]] =
    val elems = bytes.iterator.map(b => Expr(b)).toList
    '{ Array[Byte](${ Varargs(elems) }*) }
}
