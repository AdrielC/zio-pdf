/*
 * Composable, fail-closed document transforms.
 *
 * A PDF transform is deliberately a document program rather than a collection
 * of unrelated helpers. Each pass receives the output document of the pass to
 * its left; rendering happens only after the complete program has succeeded.
 * This is especially important for font work, where a failed compatibility
 * check must never leave a caller with a partly rewritten byte stream.
 */

package zio.pdf

import zio.{Chunk, NonEmptyChunk, ZIO}
import zio.blocks.chunk.{Chunk as BlocksChunk, ChunkMap}
import zio.stream.ZStream

/**
 * A composable PDF document transform with a typed result.
 *
 * The executable structure is an immutable, inspectable [[PdfTransform.Plan]],
 * while `result` is the typed projection from the interpreter's immutable
 * report state. That keeps `>>>` analyzable without sacrificing result types.
 */
final class PdfTransform[+A] private[pdf] (
  private[pdf] val plan: PdfTransform.Plan,
  private[pdf] val result: PdfTransform.Result[A]
) { self =>

  import PdfTransform.*

  /** Transform the typed result without changing the rewritten document. */
  def map[B](f: A => B): PdfTransform[B] =
    new PdfTransform(plan, result.map(f))

  /** Run `that` against the document produced by this transform. */
  def andThen[B](that: PdfTransform[B]): PdfTransform[(A, B)] =
    new PdfTransform(plan.andThen(that.plan), result.zip(that.result))

  /** Arrow-style composition that keeps the report of the right-hand pass. */
  infix def >>>[B](that: PdfTransform[B]): PdfTransform[B] =
    new PdfTransform(plan.andThen(that.plan), that.result)

  /** Static execution facts derived by interpreting the same public plan. */
  def profile: Profile =
    PdfTransform.profile(plan)

  /** Inspect the plan or run it through a separate static analysis. */
  def program: PdfTransform.Plan = plan

  /**
   * Decode once, run the full transform program, then expose a streaming
   * re-encoding plus the typed report. No output bytes exist on a failed pass.
   */
  def run[R](
    source: ZStream[R, Throwable, Byte],
    options: PdfEngine.Options = PdfEngine.Options.default
  ): ZIO[R & PdfEngine, Throwable, Output[A]] =
    PdfEngine.decode(source.via(PdfEngine.materializedInputLimit(options)), options).runCollect.flatMap { decoded =>
      ZIO.fromEither(Document.fromDecoded(decoded)).flatMap { document =>
        ZIO.fromEither(PdfTransform.compile(plan, document)).flatMap { case (runtime, rewritten) =>
          ZIO.fromEither(result.read(runtime)).map(value => Output(value, rewritten.render))
        }
      }
    }
}

object PdfTransform {

  /** A successful transform report paired with a lazy, streaming PDF encoder. */
  final case class Output[+A](value: A, bytes: ZStream[Any, Throwable, Byte])

  /**
   * Opaque document context carried between plan operations.
   *
   * The parser owns the concrete document graph. Public plans can be analyzed
   * without making that mutable-looking implementation detail part of the API.
   */
  sealed trait Context

  private[pdf] sealed trait Result[+A] {
    private[pdf] def read(runtime: Runtime): Either[Throwable, A]

    final def map[B](f: A => B): Result[B] =
      Result.Map(this, f)

    final def zip[B](that: Result[B]): Result[(A, B)] =
      Result.Pair(this, that)
  }

  private[pdf] object Result {
    final case class Map[A, B](source: Result[A], f: A => B) extends Result[B] {
      private[pdf] def read(runtime: Runtime): Either[Throwable, B] = source.read(runtime).map(f)
    }

    final case class Pair[A, B](left: Result[A], right: Result[B]) extends Result[(A, B)] {
      private[pdf] def read(runtime: Runtime): Either[Throwable, (A, B)] =
        left.read(runtime).flatMap(leftValue => right.read(runtime).map(rightValue => (leftValue, rightValue)))
    }
  }

  /** A private identity key keeps typed operation reports in immutable interpreter state. */
  private[pdf] final class ResultSlot[A] private[PdfTransform] () extends Result[A] {
    private[pdf] def read(runtime: Runtime): Either[Throwable, A] = runtime.get(this)
  }

  private[pdf] final case class Runtime(results: Map[ResultSlot[?], Any]) {
    def put[A](slot: ResultSlot[A], value: A): Runtime =
      copy(results = results.updated(slot, value))

    def get[A](slot: ResultSlot[A]): Either[Throwable, A] =
      results.get(slot) match {
        case Some(value) => Right(value.asInstanceOf[A])
        case None        => Left(new IllegalStateException("transform plan did not produce its declared report"))
      }
  }

  private[pdf] object Runtime {
    val empty: Runtime = Runtime(Map.empty)
  }

  /**
   * The transform algebra. These are semantic leaves, not wrapped evaluator
   * closures, so the plan can be compiled or analyzed independently.
   */
  enum Op[A, B]:
    case RemapExistingFonts(
      fromBaseFont: String,
      toBaseFont: String,
      private[pdf] val result: ResultSlot[fonts.Replacement]
    ) extends Op[Context, Context]
    case TokenizeText[Token](
      tokenizer: text.Tokenizer[Token],
      private[pdf] val result: ResultSlot[Chunk[text.PageTokens[Token]]]
    ) extends Op[Context, Context]

  /** A caller-supplied static interpretation of one named transform operation. */
  trait Analyzer[+A] {
    def apply(op: Op[Context, Context]): A
  }

  /** The minimal combination law needed to analyze a composed plan. */
  trait Monoid[A] {
    def empty: A
    def combine(left: A, right: A): A
  }

  /** The one immutable structural program used by document transforms. */
  final case class Plan private (operations: Chunk[Op[Context, Context]]) {
    def andThen(right: Plan): Plan = Plan(operations ++ right.operations)
    infix def >>>(right: Plan): Plan = andThen(right)
    def size: Int = operations.size

    def analyze[A](analyzer: Analyzer[A])(using monoid: Monoid[A]): A =
      operations.foldLeft(monoid.empty)((acc, op) => monoid.combine(acc, analyzer(op)))
  }

  object Plan {
    val empty: Plan = Plan(Chunk.empty)
    def single(op: Op[Context, Context]): Plan = Plan(Chunk.single(op))
  }

  /** Static facts derived from the embedded operations, not sidecar metadata. */
  final case class Profile(
    operations: Chunk[String],
    requiresMaterializedDocument: Boolean,
    readsContentStreams: Boolean
  )

  object Profile {
    val empty: Profile = Profile(Chunk.empty, requiresMaterializedDocument = false, readsContentStreams = false)

    given Monoid[Profile] with {
      def empty: Profile = Profile.empty

      def combine(left: Profile, right: Profile): Profile =
        Profile(
          left.operations ++ right.operations,
          left.requiresMaterializedDocument || right.requiresMaterializedDocument,
          left.readsContentStreams || right.readsContentStreams
        )
    }
  }

  private val profileAnalysis: Analyzer[Profile] = new Analyzer[Profile] {
    def apply(op: Op[Context, Context]): Profile =
      op match {
        case Op.RemapExistingFonts(_, _, _) =>
          Profile(Chunk("remap-existing-fonts"), requiresMaterializedDocument = true, readsContentStreams = false)
        case Op.TokenizeText(_, _) =>
          Profile(Chunk("tokenize-text"), requiresMaterializedDocument = true, readsContentStreams = true)
      }
  }

  /** Derive execution facts by analyzing the same public plan. */
  def profile(plan: Plan): Profile = plan.analyze(profileAnalysis)

  private def compile(plan: Plan, initial: Document): Either[Throwable, (Runtime, Document)] = {
    val iterator = plan.operations.iterator
    var runtime  = Runtime.empty
    var document = initial
    var failure: Option[Throwable] = None

    while iterator.hasNext && failure.isEmpty do
      iterator.next() match {
        case Op.RemapExistingFonts(fromBaseFont, toBaseFont, slot) =>
          fonts.replaceExistingDocument(document, fromBaseFont, toBaseFont) match {
            case Left(error) => failure = Some(error)
            case Right(prepared) =>
              runtime = runtime.put(slot, prepared.value)
              document = prepared.document
          }
        case Op.TokenizeText(tokenizer, slot) =>
          runtime = runtime.put(slot, text.tokenizeDocument(document, tokenizer))
      }

    failure match {
      case Some(error) => Left(error)
      case None        => Right((runtime, document))
    }
  }

  private def operation[A](op: Op[Context, Context], result: Result[A]): PdfTransform[A] =
    new PdfTransform(Plan.single(op), result)

  /** Errors raised by fail-closed document transforms. */
  sealed abstract class Error(message: String) extends RuntimeException(message)

  object Error {
    enum FontRole(val label: String) {
      case Source extends FontRole("source")
      case Replacement extends FontRole("replacement")
    }

    final case class MissingTrailer()
        extends Error("cannot rewrite a PDF without a trailer or xref-stream trailer")

    final case class SourceFontNotFound(baseFont: String)
        extends Error(s"no document font has /BaseFont /$baseFont")

    final case class TargetFontNotFound(baseFont: String)
        extends Error(s"no replacement document font has /BaseFont /$baseFont")

    final case class AmbiguousTargetFont(baseFont: String, objectNumbers: Chunk[Long])
        extends Error(s"/BaseFont /$baseFont resolves to more than one document font: ${objectNumbers.mkString(", ")}")

    /**
     * A replacement cannot preserve existing glyph codes for this font kind.
     *
     * The fields are deliberately exposed so callers can turn the failure into
     * an API response or structured log event without parsing the message.
     */
    final case class UnsupportedFontSubtype(
      role: FontRole,
      objectNumber: Long,
      baseFont: String,
      subtype: Option[String]
    ) extends Error(
          s"cannot replace /BaseFont /$baseFont: ${role.label} font object $objectNumber has " +
            s"${subtype.fold("no /Subtype")(value => s"/Subtype /$value")}. " +
            "replaceExisting preserves the existing content-stream glyph codes and supports /Type1, " +
            "/TrueType, and verified /Type0 resource pairs. Other composite/CID fonts require glyph " +
            "re-encoding, shaping, and font-program embedding; use text.tokenize for extraction-only " +
            "work or a font-program writer for a visual replacement."
        )

    final case class MetricsUnavailable(sourceObjectNumber: Long, targetObjectNumber: Long, missing: Chunk[String])
        extends Error(s"font objects $sourceObjectNumber and $targetObjectNumber lack explicit layout metadata: ${missing.mkString(", ")}")

    final case class IncompatibleFont(objectNumber: Long, replacementObjectNumber: Long, field: String)
        extends Error(
          s"font object $objectNumber is not code-and-metric compatible with $replacementObjectNumber at /$field"
        )

    final case class InvalidToUnicode(fontObjectNumber: Long, cmapObjectNumber: Option[Long])
        extends Error(s"font object $fontObjectNumber has an unreadable /ToUnicode CMap${cmapObjectNumber.fold("")(number => s" at object $number")}")

    final case class CompositeFontDataUnavailable(fontObjectNumber: Long, field: String)
        extends Error(s"composite font object $fontObjectNumber lacks a usable /$field required for an existing-resource remap")
  }

  private[pdf] final case class Prepared[+A](document: Document, value: A)

  private[pdf] final case class FontRecord(index: Obj.Index, data: Prim.Dict)

  /**
   * Decoded document state retained by structural transforms. Content payloads
   * remain compressed and lazy; only text observers inflate page streams or
   * ToUnicode CMaps they actually need.
   */
  private[pdf] final case class Document(
    elements: Chunk[Element],
    trailer: Trailer,
    version: Option[Version]
  ) extends Context {

    def fonts: Chunk[FontRecord] =
      elements.collect {
        case Element.Data(Obj(index, data @ Prim.Dict(_)), _)
            if data("Type").contains(Prim.Name("Font")) =>
          FontRecord(index, data)
      }

    def rebindableFonts: Chunk[FontRecord] =
      fonts.filter(PdfTransform.fonts.isRebindable)

    def font(objectNumber: Long): Option[FontRecord] =
      fonts.find(_.index.number == objectNumber)

    /** Reclassify changed dictionaries so observers see the transformed tree. */
    def rewriteData(
      rewrite: Prim => (Prim, Long)
    ): Either[Throwable, (Document, Long)] = {
      val out = Chunk.newBuilder[Element]
      val it  = elements.iterator
      var updates = 0L
      var failure: Option[Throwable] = None

      while it.hasNext && failure.isEmpty do
        it.next() match {
          case element @ Element.Data(obj, _) =>
            val (data, changed) = rewrite(obj.data)
            updates += changed
            if data == obj.data then out += element
            else
              Elements.classifyOne(Decoded.DataObj(Obj(obj.index, data))) match {
                case Right(element) => out += element
                case Left(error)    => failure = Some(error)
              }
          case element => out += element
        }

      failure match {
        case Some(error) => Left(error)
        case None        => Right((copy(elements = out.result()), updates))
      }
    }

    def remapFontResources(replacements: Map[Long, Prim.Ref]): Either[Throwable, (Document, Long)] =
      rewriteData { data =>
        def rewriteBindings(value: Prim): (Prim, Long) =
          value match {
            case Prim.Dict(bindings) =>
              val rewritten = bindings.iterator.map { case (name, font) =>
                font match {
                  case ref: Prim.Ref =>
                    replacements.get(ref.number) match {
                      case Some(target) => (name, target, 1L)
                      case None         => (name, font, 0L)
                    }
                  case _ => (name, font, 0L)
                }
              }.toList
              val count = rewritten.foldLeft(0L)((total, entry) => total + entry._3)
              Prim.Dict(ChunkMap.from(rewritten.map(entry => entry._1 -> entry._2))) -> count
            case other => other -> 0L
          }

        def loop(value: Prim): (Prim, Long) =
          value match {
            case Prim.Dict(entries) =>
              val rewritten = entries.iterator.map { case (key, value) =>
                val (next, count) =
                  if key == "Font" then rewriteBindings(value)
                  else loop(value)
                (key, next, count)
              }.toList
              val count = rewritten.foldLeft(0L)((total, entry) => total + entry._3)
              Prim.Dict(ChunkMap.from(rewritten.map(entry => entry._1 -> entry._2))) -> count
            case Prim.Array(values) =>
              val rewritten = values.iterator.map(loop).toList
              val count = rewritten.foldLeft(0L)((total, entry) => total + entry._2)
              Prim.Array(BlocksChunk.fromIterable(rewritten.map(_._1))) -> count
            case other => other -> 0L
          }

        loop(data)
      }

    def toUnicode(font: FontRecord): Either[Throwable, Option[TextExtract.ToUnicode]] =
      font.data("ToUnicode") match {
        case None => Right(None)
        case Some(Prim.Ref(number, _)) =>
          elements.collectFirst {
            case Element.Content(Obj(index, _), _, stream, _) if index.number == number => stream
          } match {
            case None => Left(Error.InvalidToUnicode(font.index.number, Some(number)))
            case Some(stream) =>
              stream.exec.toEither.left.map(error => new RuntimeException(error.messageWithContext)).flatMap { bits =>
                TextExtract.ToUnicode.parse(bits).toRight(Error.InvalidToUnicode(font.index.number, Some(number)))
              }.map(Some(_))
          }
        case Some(_) => Left(Error.InvalidToUnicode(font.index.number, None))
      }

    /** Render from the transformed object graph while generating a fresh xref. */
    def render: ZStream[Any, Throwable, Byte] = {
      val parts = Chunk.newBuilder[Part[Trailer]]
      version.foreach(value => parts += Part.Version(value))
      elements.foreach {
        case Element.Data(obj, _) =>
          parts += Part.Obj(IndirectObj(obj, None))
        case Element.Content(obj, rawStream, _, _) =>
          parts += Part.Obj(IndirectObj(obj, Some(rawStream)))
        case Element.Meta(_, _) =>
          ()
      }
      parts += Part.Meta(trailer)

      ZStream
        .fromChunk(parts.result())
        .via(WritePdf.parts)
        .mapConcat(bytes => Chunk.fromArray(bytes.toArray))
    }
  }

  private[pdf] object Document {
    def fromDecoded(decoded: Chunk[Decoded]): Either[Throwable, Document] = {
      val elements = Chunk.newBuilder[Element]
      val it       = decoded.iterator
      var trailer: Option[Trailer] = None
      var version: Option[Version] = None
      var failure: Option[Throwable] = None

      while it.hasNext && failure.isEmpty do
        val value = it.next()
        Elements.classifyOne(value) match {
          case Left(error) => failure = Some(error)
          case Right(element) =>
            elements += element
            value match {
              case Decoded.Meta(xrefs, foundTrailer, foundVersion) =>
                val fromXrefs = NonEmptyChunk.fromIterableOption(xrefs.map(_.trailer)).map(Trailer.sanitize)
                trailer = foundTrailer.orElse(fromXrefs).orElse(trailer)
                version = foundVersion.orElse(version)
              case _ => ()
            }
        }

      failure match {
        case Some(error) => Left(error)
        case None =>
          trailer match {
            case Some(value) => Right(Document(elements.result(), value, version))
            case None        => Left(Error.MissingTrailer())
          }
      }
    }
  }

  object text {

    /** Caller-owned tokenization over resolved Unicode page text. */
    trait Tokenizer[+A] {
      def tokenize(text: String): Chunk[A]
    }

    object Tokenizer {
      def from[A](f: String => Chunk[A]): Tokenizer[A] =
        new Tokenizer[A] {
          def tokenize(text: String): Chunk[A] = f(text)
        }

      /** One Unicode scalar value per token. */
      val characters: Tokenizer[Char] =
        from(text => Chunk.fromIterable(text))

      /** Conservative whitespace-delimited words, useful for quick indexing. */
      val words: Tokenizer[String] =
        from(text => Chunk.fromIterable(text.split("\\s+").iterator.filter(_.nonEmpty).toList))
    }

    /** Tokens retain their logical PDF page object for citations and indexing. */
    final case class PageTokens[+A](pageObjectNumber: Long, tokens: Chunk[A])

    /**
     * Tokenize resolved `/ToUnicode` text. This observer can be placed after
     * a font transform, so its tokens describe the document that will render.
     */
    def tokenize[A](tokenizer: Tokenizer[A]): PdfTransform[Chunk[PageTokens[A]]] = {
      val result = new ResultSlot[Chunk[PageTokens[A]]]
      operation(Op.TokenizeText(tokenizer, result), result)
    }

    private[pdf] def tokenizeDocument[A](
      document: Document,
      tokenizer: Tokenizer[A]
    ): Chunk[PageTokens[A]] =
      TextExtract.fromElements(document.elements).map { page =>
        PageTokens(page.pageNumber, tokenizer.tokenize(page.text))
      }

    /**
     * Streaming form for indexing-only work. Unlike the document-transform
     * form above, it creates no rewrite plan or output PDF; it maps the
     * existing text-extraction stream directly into caller-owned tokens.
     */
    def tokenize[R, A](
      source: ZStream[R, Throwable, Byte],
      tokenizer: Tokenizer[A],
      options: PdfEngine.Options = PdfEngine.Options.default
    ): ZStream[R & PdfEngine, Throwable, PageTokens[A]] =
      PdfEngine.extractText(source, options).map(page => PageTokens(page.pageNumber, tokenizer.tokenize(page.text)))
  }

  object fonts {

    /** Result of a verified font-resource remap. */
    final case class Replacement(
      sourceBaseFont: String,
      targetBaseFont: String,
      sourceObjectNumbers: Chunk[Long],
      targetObjectNumber: Long,
      resourceBindingsRewritten: Long
    )

    private val simpleSubtypes = Set("Type1", "TrueType")
    private val compositeSubtype = "Type0"
    private val layoutFields = Chunk("Encoding", "FirstChar", "LastChar", "Widths")
    private val cidParentFields = Chunk("Encoding")
    private val cidRequiredFields = Chunk("CIDSystemInfo")
    private val cidMetricFields = Chunk("DW", "W", "DW2", "W2", "CIDToGIDMap")

    /**
     * Remap every `/Font` resource pointing at `/BaseFont fromBaseFont` to the
     * one existing document font named `toBaseFont`.
     *
     * This is a genuine font replacement, not a `/BaseFont` string mutation:
     * page content keeps its glyph codes while the resource points at the
     * replacement font object. It is fail-closed unless the simple-font
     * subtype, encoding, widths, and optional `/ToUnicode` mapping agree.
     * `/Type0` resources are also accepted when their CMap and CID metrics
     * prove that the same content-stream codes keep their text and layout
     * meaning. Other composite/CID fonts and external font embedding require
     * a font-program writer and are rejected rather than approximated.
     */
    def replaceExisting(fromBaseFont: String, toBaseFont: String): PdfTransform[Replacement] = {
      val result = new ResultSlot[Replacement]
      operation(Op.RemapExistingFonts(fromBaseFont, toBaseFont, result), result)
    }

    private[pdf] def replaceExistingDocument(
      document: Document,
      fromBaseFont: String,
      toBaseFont: String
    ): Either[Throwable, Prepared[Replacement]] = {
      val allFonts = document.rebindableFonts
      val sources = allFonts.filter(record => baseFont(record.data).contains(fromBaseFont))
      val targets = allFonts.filter(record => baseFont(record.data).contains(toBaseFont))

      if sources.isEmpty then Left(Error.SourceFontNotFound(fromBaseFont))
      else if targets.isEmpty then Left(Error.TargetFontNotFound(toBaseFont))
      else if targets.size != 1 then
        Left(Error.AmbiguousTargetFont(toBaseFont, targets.map(_.index.number)))
      else {
        val target = targets.head
        sources.foldLeft[Either[Throwable, Unit]](Right(())) { (checked, source) =>
          checked.flatMap(_ => compatible(document, source, target))
        }.flatMap { _ =>
          val replacements = sources.iterator.map(source => source.index.number -> Prim.Ref(
            target.index.number,
            target.index.generation
          )).toMap
          document.remapFontResources(replacements).map { case (rewritten, bindings) =>
            Prepared(
              rewritten,
              Replacement(
                fromBaseFont,
                toBaseFont,
                sources.map(_.index.number),
                target.index.number,
                bindings
              )
            )
          }
        }
      }
    }

    private def compatible(
      document: Document,
      source: FontRecord,
      target: FontRecord
    ): Either[Throwable, Unit] = {
      val sourceSubtype = baseName(source.data, "Subtype")
      val targetSubtype = baseName(target.data, "Subtype")

      if !sourceSubtype.exists(isSupportedSubtype) then
        Left(
          Error.UnsupportedFontSubtype(
            Error.FontRole.Source,
            source.index.number,
            baseFont(source.data).getOrElse("unknown"),
            sourceSubtype
          )
        )
      else if !targetSubtype.exists(isSupportedSubtype) then
        Left(
          Error.UnsupportedFontSubtype(
            Error.FontRole.Replacement,
            target.index.number,
            baseFont(target.data).getOrElse("unknown"),
            targetSubtype
          )
        )
      else if sourceSubtype != targetSubtype then
        Left(Error.IncompatibleFont(source.index.number, target.index.number, "Subtype"))
      else if sourceSubtype.contains(compositeSubtype) then
        compatibleComposite(document, source, target)
      else
        compatibleSimple(document, source, target)
    }

    private[pdf] def isRebindable(record: FontRecord): Boolean =
      baseName(record.data, "Subtype").exists(isSupportedSubtype)

    private def isSupportedSubtype(subtype: String): Boolean =
      simpleSubtypes.contains(subtype) || subtype == compositeSubtype

    private def compatibleSimple(
      document: Document,
      source: FontRecord,
      target: FontRecord
    ): Either[Throwable, Unit] =
      for {
        _ <- matchingFields(source, target, source.data, target.data, layoutFields)
        _ <- matchingToUnicode(document, source, target)
      } yield ()

    private def compatibleComposite(
      document: Document,
      source: FontRecord,
      target: FontRecord
    ): Either[Throwable, Unit] =
      for {
        _                <- matchingFields(source, target, source.data, target.data, cidParentFields)
        sourceDescendant <- descendant(document, source)
        targetDescendant <- descendant(document, target)
        _ <- Either.cond(
          baseName(sourceDescendant.data, "Subtype") == baseName(targetDescendant.data, "Subtype"),
          (),
          Error.IncompatibleFont(source.index.number, target.index.number, "DescendantFonts/Subtype")
        )
        _ <- matchingFields(
          source,
          target,
          sourceDescendant.data,
          targetDescendant.data,
          cidRequiredFields
        )
        _ <- matchingFields(
          source,
          target,
          sourceDescendant.data,
          targetDescendant.data,
          cidMetricFields,
          requireAll = false
        )
        _ <- matchingToUnicode(document, source, target)
      } yield ()

    private def descendant(document: Document, font: FontRecord): Either[Throwable, FontRecord] =
      val references = font.data("DescendantFonts") match
        case Some(Prim.Array(values)) => values.collect { case reference: Prim.Ref => reference }
        case _                        => Chunk.empty

      if references.size != 1 then Left(Error.CompositeFontDataUnavailable(font.index.number, "DescendantFonts"))
      else
        document
          .font(references.head.number)
          .toRight(Error.CompositeFontDataUnavailable(font.index.number, "DescendantFonts"))

    private def matchingFields(
      source: FontRecord,
      target: FontRecord,
      sourceData: Prim.Dict,
      targetData: Prim.Dict,
      fields: Chunk[String],
      requireAll: Boolean = true
    ): Either[Throwable, Unit] = {
      val missing =
        if requireAll then fields.filter(field => sourceData(field).isEmpty || targetData(field).isEmpty)
        else Chunk.empty
      if missing.nonEmpty then Left(Error.MetricsUnavailable(source.index.number, target.index.number, missing))
      else
        fields.find(field => sourceData(field) != targetData(field)) match {
          case Some(field) => Left(Error.IncompatibleFont(source.index.number, target.index.number, field))
          case None        => Right(())
        }
    }

    private def matchingToUnicode(
      document: Document,
      source: FontRecord,
      target: FontRecord
    ): Either[Throwable, Unit] =
      for {
        sourceMap <- document.toUnicode(source)
        targetMap <- document.toUnicode(target)
        _ <- Either.cond(
          sourceMap == targetMap,
          (),
          Error.IncompatibleFont(source.index.number, target.index.number, "ToUnicode")
        )
      } yield ()

    private def baseFont(data: Prim.Dict): Option[String] =
      baseName(data, "BaseFont")

    private def baseName(data: Prim.Dict, field: String): Option[String] =
      data(field).collect { case Prim.Name(value) => value }
  }
}
