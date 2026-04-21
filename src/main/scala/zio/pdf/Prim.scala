/*
 * Port of fs2.pdf.Prim to Scala 3 + scodec 2.3.
 *
 * Algebra for PDF primitive values. Each top-level indirect object
 * carries one of these (most often a `Dict`). Codecs follow the
 * legacy semantics very closely: a manually-ordered choice instead
 * of `Codec.coproduct` because a `Ref` would otherwise be
 * mis-decoded as a `Number`.
 */

package zio.pdf

import scala.util.Try

import zio.pdf.codec.{Codecs, Many, Text, Whitespace}
import _root_.scodec.{Attempt, Codec, DecodeResult, Decoder, Encoder, Err}
import _root_.scodec.bits.{BitVector, ByteVector}
import _root_.scodec.codecs.*

sealed trait Prim

object Prim extends PrimCodec {

  case object Null extends Prim

  /** Indirect reference: `<num> <gen> R`. */
  final case class Ref(number: Long, generation: Int) extends Prim

  final case class Bool(value: Boolean) extends Prim

  final case class Number(data: BigDecimal) extends Prim

  /** Name: `/Foo`. Used for dict keys and identifiers. */
  final case class Name(data: String) extends Prim

  /** Literal string: `(text)`. */
  final case class Str(data: ByteVector) extends Prim

  /** Hex string: `<deadbeef>`. */
  final case class HexStr(data: ByteVector) extends Prim

  /** PDF array: `[1 2 3 (foo) /Bar]`. */
  final case class Array(data: List[Prim]) extends Prim

  object Array {
    def refs(nums: Long*): Array =
      Array(nums.map(refNum).toList)
    def nums(ns: BigDecimal*): Array =
      Array(ns.map(Number(_)).toList)
  }

  /** PDF dict: `<< /Key /Value >>`. */
  final case class Dict(data: Map[String, Prim]) extends Prim {
    def apply(key: String): Option[Prim]              = data.get(key)
    def ++(other: Dict): Dict                          = Dict(data ++ other.data)
    def ref(key: String): Option[Ref]                  =
      apply(key).collect { case r @ Ref(_, _) => r }
  }

  object Dict {

    def empty: Dict = Dict(Map.empty)

    def attempt(key: String)(data: Prim): Attempt[Prim] =
      tryDict(key)(data) match {
        case Some(p) => Attempt.successful(p)
        case None    => Attempt.failure(Err(s"key `$key` not present in $data"))
      }

    def path[A](keys: String*)(data: Prim)(extract: PartialFunction[Prim, A]): Attempt[A] = {
      val keyList = keys.toList
      val joined  = keyList.mkString("->")
      keyList.foldLeft[Attempt[Prim]](Attempt.successful(data)) { (acc, key) =>
        acc.flatMap(attempt(key))
      }.flatMap { result =>
        extract.lift(result) match {
          case Some(a) => Attempt.successful(a)
          case None    => Attempt.failure(Err(s"extraction failed for `$joined`"))
        }
      }
    }

    def name(keys: String*)(data: Prim): Attempt[String] =
      path(keys*)(data) { case Name(d) => d }

    def number(keys: String*)(data: Prim): Attempt[BigDecimal] =
      path(keys*)(data) { case Number(d) => d }

    def array(keys: String*)(data: Prim): Attempt[List[Prim]] =
      path(keys*)(data) { case Array(d) => d }

    def numbers(keys: String*)(data: Prim): Attempt[List[BigDecimal]] =
      array(keys*)(data).flatMap { ps =>
        ps.foldRight[Attempt[List[BigDecimal]]](Attempt.successful(Nil)) {
          case (Number(d), Attempt.Successful(acc)) => Attempt.successful(d :: acc)
          case (other, Attempt.Successful(_))       => Attempt.failure(Err(s"wrong type: $other"))
          case (_, f @ Attempt.Failure(_))          => f
        }
      }

    def collectRefs(keys: String*)(data: Prim): Attempt[List[Long]] =
      array(keys*)(data).map(_.collect { case Ref(n, _) => n })

    def updated(key: String, value: Prim)(d: Dict): Dict =
      Dict(d.data.updated(key, value))

    def appendOrCreateArray(key: String)(elem: Prim)(d: Dict): Dict =
      Dict(d.data.updatedWith(key)(old => Some(Prim.appendOrCreateArray(elem)(old))))

    def appendOrCreateDict(key: String)(elemKey: String, elem: Prim)(d: Dict): Dict =
      Dict(d.data.updatedWith(key)(old => Some(Prim.appendOrCreateDict(elemKey, elem)(old))))

    def deepMergeValues(key: String): (Prim, Prim) => Attempt[Prim] = {
      case (update @ Dict(_), original @ Dict(_)) =>
        deepMerge(update)(original)
      case (update, original @ Dict(_)) =>
        Codecs.fail(s"incompatible types for Prim.Dict.deepMerge: $key | $update | $original")
      case (Array(update), Array(original)) =>
        Attempt.successful(Array(original ++ update))
      case (update, Array(original)) =>
        Attempt.successful(Array(original :+ update))
      case (update, original) =>
        Codecs.fail(s"incompatible types for Prim.Dict.deepMerge: $key | $update | $original")
    }

    def deepMerge(update: Dict)(original: Dict): Attempt[Dict] =
      update.data.toList.foldLeft[Attempt[Dict]](Attempt.successful(original)) {
        case (Attempt.Successful(z), (key, updateValue)) =>
          z.data.get(key) match {
            case Some(originalValue) =>
              deepMergeValues(key)(updateValue, originalValue).map(updated(key, _)(z))
            case None =>
              Attempt.successful(updated(key, updateValue)(z))
          }
        case (f @ Attempt.Failure(_), _) => f
      }
  }

  def appendOrCreateArray(elem: Prim): Option[Prim] => Prim = {
    case Some(Array(old))           => Array(old :+ elem)
    case Some(old @ Ref(_, _))      => Array(List(old, elem))
    case _                           => Array(List(elem))
  }

  def appendOrCreateDict(key: String, elem: Prim): Option[Prim] => Prim = {
    case Some(Dict(old)) => Dict(old + (key -> elem))
    case _                => Dict(Map(key -> elem))
  }

  def withDict[A]: Prim => A => Option[(A, Dict)] = {
    case data @ Dict(_) => a => Some((a, data))
    case _              => _ => None
  }

  def liftWithKey(key: String)(data: Prim): Option[(String, Dict)] =
    tryDict(key)(data)
      .collect { case Name(t) => t }
      .flatMap(withDict(data))

  object tpe {
    def unapply(data: Prim): Option[(String, Dict)] = liftWithKey("Type")(data)
  }

  object subtype {
    def unapply(data: Prim): Option[(String, Dict)] = liftWithKey("Subtype")(data)
  }

  object filter {
    def unapply(data: Prim): Option[(String, Dict)] = liftWithKey("Filter")(data)
  }

  object contents {
    def unapply(data: Prim): Option[Prim] = tryDict("Contents")(data)
  }

  object linearization {
    def unapply(data: Prim): Boolean = tryDict("Linearized")(data).isDefined
  }

  object fontResources {
    def unapply(prim: Prim): Option[Dict] = prim match {
      case d @ Dict(data) if data.contains("Font") => Some(d)
      case _                                        => None
    }
  }

  object refs {
    def unapply(data: Prim): Option[List[Ref]] = data match {
      case Array(elems) =>
        val refsOnly = elems.collect { case r @ Ref(_, _) => r }
        if (refsOnly.size == elems.size) Some(refsOnly) else None
      case _ => None
    }
  }

  def path[A](keys: String*)(data: Prim)(extract: PartialFunction[Prim, A]): Option[A] =
    keys.toList
      .foldLeft[Option[Prim]](Some(data))((acc, key) => acc.flatMap(tryDict(key)))
      .flatMap(extract.lift)

  def dictOnly: Prim => Option[Map[String, Prim]] = {
    case Dict(d) => Some(d)
    case _       => None
  }

  def containsName(key: String)(name: String)(data: Dict): Boolean =
    data.data.get(key) match {
      case Some(Name(actual))  => actual == name
      case Some(Array(elems))  => elems.exists { case Name(actual) => actual == name; case _ => false }
      case _                    => false
    }

  def dict(values: (String, Prim)*): Dict = Dict(values.toMap)

  def array(values: Prim*): Prim = Array(values.toList)

  def refI(to: Obj.Index): Prim    = Ref(to.number, to.generation)
  def refNum(to: Long): Prim        = Ref(to, 0)
  def refT(to: IndirectObj): Prim   = refI(to.obj.index)
  def ref(to: Obj): Prim            = refI(to.index)

  def num(n: BigDecimal): Prim = Number(n)

  def str(s: String): Str       = Str(ByteVector(s.getBytes))
  def hexStr(s: String): HexStr = HexStr(ByteVector(s.getBytes))

  def tryDict(key: String): Prim => Option[Prim] = {
    case Dict(d) => d.get(key)
    case _       => None
  }

  def isType(tpe: String)(data: Map[String, Prim]): Boolean =
    data.get("Type").contains(Name(tpe))
}

private[pdf] trait PrimCodec {
  import _root_.scodec.codecs.{lazily, optional, recover}
  import Whitespace.{skipWs, space, ws}
  import Codecs.{bracketChar, opt}
  import Text.{ascii, char, ranges, str}

  val Codec_Null: Codec[Prim.Null.type] =
    str("null").xmap(_ => Prim.Null, _ => ())

  val Codec_Ref: Codec[Prim.Ref] =
    ((ascii.long <~ space) :: (ascii.int <~ space <~ char('R')))
      .xmap({ case (n, g) => Prim.Ref(n, g) }, r => (r.number, r.generation))

  private val encodeBool: Encoder[Prim.Bool] = Encoder {
    case Prim.Bool(true)  => Attempt.successful(BitVector("true".getBytes))
    case Prim.Bool(false) => Attempt.successful(BitVector("false".getBytes))
  }

  private val decodeBool: Decoder[Prim.Bool] =
    Decoder.choiceDecoder(
      str("true").map(_ => Prim.Bool(true)),
      str("false").map(_ => Prim.Bool(false))
    )

  val Codec_Bool: Codec[Prim.Bool] = Codec(encodeBool, decodeBool)

  private def formatNumber(parts: ((Option[Unit], String), Option[String])): String = {
    val ((minus, pre), post) = parts
    val frac                  = post.fold("")(p => s".$p")
    val m                     = minus.fold("")(_ => "-")
    s"$m$pre$frac"
  }

  private def splitNumber(num: Prim.Number): Attempt[((Option[Unit], String), Option[String])] =
    num.data.toString.split(raw"\.").toList match {
      case List(pre, post) => Attempt.successful(((None, pre), Some(post)))
      case List(n)         => Attempt.successful(((None, n), None))
      case other           => Codecs.fail(s"invalid number: $num ($other)")
    }

  val Codec_Number: Codec[Prim.Number] =
    (
      opt(using char('-')) :: ascii.digits1 :: optional(recover(char('.')), ascii.digits1)
    ).xmap[((Option[Unit], String), Option[String])](
      tup => ((tup._1, tup._2), tup._3),
      { case ((m, pre), post) => (m, pre, post) }
    ).exmap(
      a => Attempt.fromTry(Try(BigDecimal(formatNumber(a)))).map(Prim.Number(_)),
      splitNumber
    )

  /** Find the closing `)` for a literal string, taking nested
    * unescaped parens and backslash escapes into account. */
  private val findClosingParen: Decoder[ByteVector] = Decoder { bits =>
    @annotation.tailrec
    def rec(parens: Int)(input: ByteVector, output: ByteVector): Attempt[(ByteVector, ByteVector)] =
      input.headOption match {
        case Some(b) if b == '\\'.toByte =>
          rec(parens)(input.drop(2), output ++ input.take(2))
        case Some(b) if b == '('.toByte =>
          rec(parens + 1)(input.drop(1), output :+ b)
        case Some(b) if b == ')'.toByte && parens > 0 =>
          rec(parens - 1)(input.drop(1), output :+ b)
        case Some(b) if b == ')'.toByte =>
          Attempt.successful((output, input))
        case Some(b) =>
          rec(parens)(input.drop(1), output :+ b)
        case None =>
          Codecs.fail("could not find closing parenthesis for Str")
      }
    rec(0)(bits.bytes, ByteVector.empty)
      .map { case (result, remainder) => DecodeResult(result, remainder.bits) }
  }

  private val strBytes: Codec[ByteVector] =
    char('(') ~> Codec(bytes, findClosingParen) <~ char(')')

  val Codec_Str: Codec[Prim.Str] =
    strBytes.xmap(Prim.Str(_), _.data)

  private val hexChar: Codec[ByteVector] =
    ranges(('a', 'f'), ('A', 'F'), ('0', '9'))

  val Codec_HexStr: Codec[Prim.HexStr] =
    bracketChar('<', '>')(hexChar).xmap(Prim.HexStr(_), _.data)

  private def trim[A](inner: Codec[A]): Codec[A] = skipWs ~> inner <~ skipWs

  val Codec_Array: Codec[Prim.Array] =
    (skipWs ~> Many.bracket(trim(char('[')), trim(char(']')))(lazily(trim(Codec_Prim)) <~ ws))
      .xmap(Prim.Array(_), _.data)
      .withContext("array")

  private val nameString: Codec[String] =
    trim(
      char('/').withContext("name solidus") ~>
        Text.charsNoneOf(Text.latin)("/<>[]( \r\n".toList).withContext("name chars")
    ).withContext("name string")

  val Codec_Name: Codec[Prim.Name] =
    nameString.xmap(Prim.Name(_), _.data).withContext("name")

  private val dictElem: Codec[(String, Prim)] =
    (Whitespace.skipWs ~> nameString.withContext("dict key") ::
      (ws ~> trim(lazily(Codec_Prim)).withContext("dict value")))
      .withContext("dict elem")

  private val dictStartMarker: Codec[Unit] =
    trim(str("<<")).withContext("dict<<")

  private val dictEndMarker: Codec[Unit] =
    (Whitespace.skipWs ~> trim(str(">>"))).withContext("dict>>")

  val Codec_Dict: Codec[Prim.Dict] =
    Many.bracket(dictStartMarker, dictEndMarker)(dictElem)
      .xmap[Map[String, Prim]](Map.from, _.toList)
      .xmap(Prim.Dict(_), _.data)
      .withContext("dict")

  // Manual choice rather than Codec.coproduct - the original code used
  // a deliberately-ordered choice so a Ref doesn't get mis-decoded as
  // a Number. Same approach here.
  given Codec[Prim] =
    Codec(
      Encoder { (p: Prim) =>
        p match {
          case n: Prim.Name      => Codec_Name.encode(n)
          case Prim.Null         => Codec_Null.encode(Prim.Null)
          case b: Prim.Bool      => Codec_Bool.encode(b)
          case r: Prim.Ref       => Codec_Ref.encode(r)
          case n: Prim.Number    => Codec_Number.encode(n)
          case s: Prim.Str       => Codec_Str.encode(s)
          case s: Prim.HexStr    => Codec_HexStr.encode(s)
          case a: Prim.Array     => Codec_Array.encode(a)
          case d: Prim.Dict      => Codec_Dict.encode(d)
        }
      },
      Decoder.choiceDecoder[Prim](
        Codec_Name.upcast[Prim],
        Codec_Null.upcast[Prim],
        Codec_Bool.upcast[Prim],
        Codec_Ref.upcast[Prim],
        Codec_Number.upcast[Prim],
        Codec_Str.upcast[Prim],
        Codec_HexStr.upcast[Prim],
        Codec_Array.upcast[Prim],
        Codec_Dict.upcast[Prim]
      )
    )

  def Codec_Prim: Codec[Prim] = summon[Codec[Prim]]
}
