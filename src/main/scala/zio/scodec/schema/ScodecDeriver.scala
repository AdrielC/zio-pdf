/*
 * Copyright (c) 2013, Scodec
 * All rights reserved.
 *
 * `Deriver[scodec.Codec]` for zio-blocks-schema. Given a
 * `Schema[A]` derived via `Schema.derived[A]`, calling
 * `schema.derive(ScodecDeriver)` produces a `scodec.Codec[A]`
 * automatically, the same way `JsonCodecDeriver` produces a JSON
 * codec from the same schema.
 *
 * This is the architectural payoff of wiring zio-blocks-schema in
 * earlier: a single `Schema.derived[Foo]` declaration gives us the
 * binary codec, JSON codec, Avro codec, etc. all at once - no more
 * hand-rolling 200 `Codec[Foo]` instances by case-class for a PDF
 * data model.
 */

package zio.scodec.schema

import _root_.scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import _root_.scodec.bits.BitVector
import _root_.scodec.codecs.*

import java.time.*
import java.util.Currency
import scala.util.{Failure, Success, Try}

import zio.blocks.chunk.ChunkBuilder
import zio.blocks.docs.Doc
import zio.blocks.schema.{DynamicValue, Lazy, Modifier, PrimitiveType, PrimitiveValue, Reflect, Term}
import zio.blocks.schema.binding.{Binding, HasBinding, RegisterOffset, Registers}
import zio.blocks.schema.derive.{Deriver, HasInstance as SchemaHasInstance}
import zio.blocks.typeid.TypeId

import scala.reflect.ClassTag

/**
 * `Deriver[Codec]` over the structural patterns from zio-blocks-schema.
 *
 * Coverage:
 *   - Primitive: every numeric primitive, plus `String` (utf8_32),
 *     `Boolean`, `Byte`, `Short`, `Char`, `Unit`. Everything else
 *     falls through to a clear failure (so the call site sees what
 *     is missing rather than getting a silently broken codec).
 *   - Record: encode each field in declaration order; decode each
 *     field, accumulate into registers, then run the constructor.
 *   - Variant: write a `uint8` discriminator tag, then the matching
 *     case codec.
 *   - Wrapper: encode by `unwrap`, decode by `wrap`.
 *   - Sequence: `int32` length + repeated `elementCodec`; when the element
 *     is `Byte`, the payload is a single raw byte run (`BitVector.view`)
 *     instead of N separate `byte` decodes (large win for blobs). For a
 *     chunk-shaped field prefer `zio.blocks.chunk.Chunk` in the record —
 *     `Schema.derived` treats `zio.Chunk` as `IndexedSeq` unless you wire
 *     an explicit `Schema.chunk` / blocks chunk type.
 *   - Map: length-prefixed list of `(K, V)` pairs.
 *   - Dynamic: recursive wire format for [[DynamicValue]] (tagged tree
 *     aligned with [[zio.blocks.schema.DynamicValueType]] indices, with
 *     [[PrimitiveValue.typeIndex]] as the inner primitive discriminator).
 */
object ScodecDeriver extends Deriver[Codec] {

  private lazy val dynamicValueCodec: Codec[DynamicValue] = Codec.lazily(mkDynamicValueCodec)

  private def mkDynamicValueCodec: Codec[DynamicValue] = new Codec[DynamicValue] {
    def sizeBound: SizeBound = SizeBound.unknown

    def encode(value: DynamicValue): Attempt[BitVector] =
      uint8.encode(value.typeIndex).flatMap { tagBits =>
        value match {
          case DynamicValue.Null =>
            Attempt.successful(tagBits)

          case p: DynamicValue.Primitive =>
            primitiveValueCodec.encode(p.value).map(tagBits ++ _)

          case r: DynamicValue.Record =>
            val n = r.fields.length
            int32.encode(n).flatMap { lenBits =>
              val init = tagBits ++ lenBits
              r.fields.foldLeft[Attempt[BitVector]](Attempt.successful(init)) {
                case (Attempt.Successful(acc), (k, v)) =>
                  utf8_32.encode(k).flatMap(kb =>
                    dynamicValueCodec.encode(v).map(acc ++ kb ++ _)
                  )
                case (f @ Attempt.Failure(_), _) => f
              }
            }

          case v: DynamicValue.Variant =>
            utf8_32.encode(v.caseNameValue).flatMap { kb =>
              dynamicValueCodec.encode(v.value).map(tagBits ++ kb ++ _)
            }

          case s: DynamicValue.Sequence =>
            val n = s.elements.length
            int32.encode(n).flatMap { lenBits =>
              val init = tagBits ++ lenBits
              s.elements.foldLeft[Attempt[BitVector]](Attempt.successful(init)) {
                case (Attempt.Successful(acc), elem) =>
                  dynamicValueCodec.encode(elem).map(acc ++ _)
                case (f @ Attempt.Failure(_), _) => f
              }
            }

          case m: DynamicValue.Map =>
            val n = m.entries.length
            int32.encode(n).flatMap { lenBits =>
              val init = tagBits ++ lenBits
              m.entries.foldLeft[Attempt[BitVector]](Attempt.successful(init)) {
                case (Attempt.Successful(acc), (k, v)) =>
                  dynamicValueCodec.encode(k).flatMap(kb =>
                    dynamicValueCodec.encode(v).map(acc ++ kb ++ _)
                  )
                case (f @ Attempt.Failure(_), _) => f
              }
            }
        }
      }

    def decode(bits: BitVector): Attempt[DecodeResult[DynamicValue]] =
      uint8.decode(bits) match {
        case Attempt.Successful(DecodeResult(tag, rest0)) =>
          tag match {
            case 5 =>
              Attempt.successful(DecodeResult(DynamicValue.Null, rest0))

            case 0 =>
              primitiveValueCodec.decode(rest0).map { case DecodeResult(pv, rem) =>
                DecodeResult(DynamicValue.Primitive(pv), rem)
              }

            case 1 =>
              int32.decode(rest0).flatMap { case DecodeResult(n, rest1) =>
                if (n < 0)
                  Attempt.failure(Err(s"ScodecDeriver: DynamicValue.Record negative field count $n"))
                else {
                  val b = ChunkBuilder.make[(String, DynamicValue)]()
                  def loop(i: Int, rem: BitVector): Attempt[DecodeResult[DynamicValue]] =
                    if (i == n) Attempt.successful(DecodeResult(DynamicValue.Record(b.result()), rem))
                    else
                      utf8_32.decode(rem).flatMap { case DecodeResult(k, r1) =>
                        dynamicValueCodec.decode(r1).flatMap { case DecodeResult(v, r2) =>
                          b += ((k, v))
                          loop(i + 1, r2)
                        }
                      }
                  loop(0, rest1)
                }
              }

            case 2 =>
              utf8_32.decode(rest0) match {
                case Attempt.Successful(DecodeResult(name, r1)) =>
                  dynamicValueCodec.decode(r1) match {
                    case Attempt.Successful(DecodeResult(inner, r2)) =>
                      Attempt.successful(DecodeResult(DynamicValue.Variant(name, inner), r2))
                    case f @ Attempt.Failure(_) => f
                  }
                case f @ Attempt.Failure(_) => f
              }

            case 3 =>
              int32.decode(rest0).flatMap { case DecodeResult(n, rest1) =>
                if (n < 0)
                  Attempt.failure(Err(s"ScodecDeriver: DynamicValue.Sequence negative length $n"))
                else {
                  val b = ChunkBuilder.make[DynamicValue]()
                  def loop(i: Int, rem: BitVector): Attempt[DecodeResult[DynamicValue]] =
                    if (i == n) Attempt.successful(DecodeResult(DynamicValue.Sequence(b.result()), rem))
                    else
                      dynamicValueCodec.decode(rem).flatMap { case DecodeResult(v, r) =>
                        b += v
                        loop(i + 1, r)
                      }
                  loop(0, rest1)
                }
              }

            case 4 =>
              int32.decode(rest0).flatMap { case DecodeResult(n, rest1) =>
                if (n < 0)
                  Attempt.failure(Err(s"ScodecDeriver: DynamicValue.Map negative entry count $n"))
                else {
                  val b = ChunkBuilder.make[(DynamicValue, DynamicValue)]()
                  def loop(i: Int, rem: BitVector): Attempt[DecodeResult[DynamicValue]] =
                    if (i == n) Attempt.successful(DecodeResult(DynamicValue.Map(b.result()), rem))
                    else
                      dynamicValueCodec.decode(rem).flatMap { case DecodeResult(k, r1) =>
                        dynamicValueCodec.decode(r1).flatMap { case DecodeResult(v, r2) =>
                          b += ((k, v))
                          loop(i + 1, r2)
                        }
                      }
                  loop(0, rest1)
                }
              }

            case other =>
              Attempt.failure(Err(s"ScodecDeriver: unknown DynamicValue tag $other"))
          }
        case f @ Attempt.Failure(_) => f
      }
  }

  private val primitiveValueCodec: Codec[PrimitiveValue] = new Codec[PrimitiveValue] {
    def sizeBound: SizeBound = SizeBound.unknown

    def encode(pv: PrimitiveValue): Attempt[BitVector] =
      uint8.encode(pv.typeIndex).flatMap { tagBits =>
        val body: Attempt[BitVector] = pv match {
          case PrimitiveValue.Unit =>
            Attempt.successful(BitVector.empty)
          case v: PrimitiveValue.Boolean =>
            bool(8).encode(v.value)
          case v: PrimitiveValue.Byte =>
            byte.encode(v.value)
          case v: PrimitiveValue.Short =>
            short16.encode(v.value)
          case v: PrimitiveValue.Int =>
            int32.encode(v.value)
          case v: PrimitiveValue.Long =>
            int64.encode(v.value)
          case v: PrimitiveValue.Float =>
            float.encode(v.value)
          case v: PrimitiveValue.Double =>
            double.encode(v.value)
          case v: PrimitiveValue.Char =>
            uint16.encode(v.value.toInt)
          case v: PrimitiveValue.String =>
            utf8_32.encode(v.value)
          case v: PrimitiveValue.BigInt =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.BigDecimal =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.DayOfWeek =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.Duration =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.Instant =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.LocalDate =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.LocalDateTime =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.LocalTime =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.Month =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.MonthDay =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.OffsetDateTime =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.OffsetTime =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.Period =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.Year =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.YearMonth =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.ZoneId =>
            utf8_32.encode(v.value.getId)
          case v: PrimitiveValue.ZoneOffset =>
            utf8_32.encode(v.value.getId)
          case v: PrimitiveValue.ZonedDateTime =>
            utf8_32.encode(v.value.toString)
          case v: PrimitiveValue.Currency =>
            utf8_32.encode(v.value.getCurrencyCode)
          case v: PrimitiveValue.UUID =>
            uuid.encode(v.value)
        }
        body.map(tagBits ++ _)
      }

    def decode(bits: BitVector): Attempt[DecodeResult[PrimitiveValue]] =
      uint8.decode(bits) match {
        case Attempt.Successful(DecodeResult(tag, rest)) =>
          tag match {
            case 0 =>
              Attempt.successful(DecodeResult(PrimitiveValue.Unit, rest))
            case 1 =>
              bool(8).decode(rest).map { case DecodeResult(b, r) => DecodeResult(PrimitiveValue.Boolean(b), r) }
            case 2 =>
              byte.decode(rest).map { case DecodeResult(b, r) => DecodeResult(PrimitiveValue.Byte(b), r) }
            case 3 =>
              short16.decode(rest).map { case DecodeResult(s, r) => DecodeResult(PrimitiveValue.Short(s), r) }
            case 4 =>
              int32.decode(rest).map { case DecodeResult(i, r) => DecodeResult(PrimitiveValue.Int(i), r) }
            case 5 =>
              int64.decode(rest).map { case DecodeResult(l, r) => DecodeResult(PrimitiveValue.Long(l), r) }
            case 6 =>
              float.decode(rest).map { case DecodeResult(f, r) => DecodeResult(PrimitiveValue.Float(f), r) }
            case 7 =>
              double.decode(rest).map { case DecodeResult(d, r) => DecodeResult(PrimitiveValue.Double(d), r) }
            case 8 =>
              uint16.decode(rest).map { case DecodeResult(n, r) => DecodeResult(PrimitiveValue.Char(n.toChar), r) }
            case 9 =>
              utf8_32.decode(rest).map { case DecodeResult(s, r) => DecodeResult(PrimitiveValue.String(s), r) }
            case 10 =>
              utf8_32.decode(rest).flatMap { case DecodeResult(s, r) =>
                Try(BigInt(s)) match {
                  case Success(bi) => Attempt.successful(DecodeResult(PrimitiveValue.BigInt(bi), r))
                  case Failure(e)  => Attempt.failure(Err(s"ScodecDeriver: invalid BigInt (${e.getMessage})"))
                }
              }
            case 11 =>
              utf8_32.decode(rest).flatMap { case DecodeResult(s, r) =>
                Try(BigDecimal(s)) match {
                  case Success(bd) => Attempt.successful(DecodeResult(PrimitiveValue.BigDecimal(bd), r))
                  case Failure(e)  => Attempt.failure(Err(s"ScodecDeriver: invalid BigDecimal (${e.getMessage})"))
                }
              }
            case 12 =>
              decodeIsoString(rest, "DayOfWeek")(s => DayOfWeek.valueOf(s))(PrimitiveValue.DayOfWeek(_))
            case 13 =>
              decodeIsoString(rest, "Duration")(Duration.parse)(PrimitiveValue.Duration(_))
            case 14 =>
              decodeIsoString(rest, "Instant")(Instant.parse)(PrimitiveValue.Instant(_))
            case 15 =>
              decodeIsoString(rest, "LocalDate")(LocalDate.parse)(PrimitiveValue.LocalDate(_))
            case 16 =>
              decodeIsoString(rest, "LocalDateTime")(LocalDateTime.parse)(PrimitiveValue.LocalDateTime(_))
            case 17 =>
              decodeIsoString(rest, "LocalTime")(LocalTime.parse)(PrimitiveValue.LocalTime(_))
            case 18 =>
              decodeIsoString(rest, "Month")(Month.valueOf)(PrimitiveValue.Month(_))
            case 19 =>
              decodeIsoString(rest, "MonthDay")(MonthDay.parse)(PrimitiveValue.MonthDay(_))
            case 20 =>
              decodeIsoString(rest, "OffsetDateTime")(OffsetDateTime.parse)(PrimitiveValue.OffsetDateTime(_))
            case 21 =>
              decodeIsoString(rest, "OffsetTime")(OffsetTime.parse)(PrimitiveValue.OffsetTime(_))
            case 22 =>
              decodeIsoString(rest, "Period")(Period.parse)(PrimitiveValue.Period(_))
            case 23 =>
              decodeIsoString(rest, "Year")(Year.parse)(PrimitiveValue.Year(_))
            case 24 =>
              decodeIsoString(rest, "YearMonth")(YearMonth.parse)(PrimitiveValue.YearMonth(_))
            case 25 =>
              decodeIsoString(rest, "ZoneId")(ZoneId.of)(PrimitiveValue.ZoneId(_))
            case 26 =>
              decodeIsoString(rest, "ZoneOffset")(ZoneOffset.of)(PrimitiveValue.ZoneOffset(_))
            case 27 =>
              decodeIsoString(rest, "ZonedDateTime")(ZonedDateTime.parse)(PrimitiveValue.ZonedDateTime(_))
            case 28 =>
              utf8_32.decode(rest).flatMap { case DecodeResult(code, r) =>
                Try(Currency.getInstance(code)) match {
                  case Success(c) => Attempt.successful(DecodeResult(PrimitiveValue.Currency(c), r))
                  case Failure(e) => Attempt.failure(Err(s"ScodecDeriver: invalid Currency (${e.getMessage})"))
                }
              }
            case 29 =>
              uuid.decode(rest).map { case DecodeResult(u, r) => DecodeResult(PrimitiveValue.UUID(u), r) }
            case other =>
              Attempt.failure(Err(s"ScodecDeriver: unknown PrimitiveValue tag $other"))
          }
        case f @ Attempt.Failure(_) => f
      }

    private def decodeIsoString[A](
      bits: BitVector,
      label: String
    )(parse: String => A)(wrap: A => PrimitiveValue): Attempt[DecodeResult[PrimitiveValue]] =
      utf8_32.decode(bits).flatMap { case DecodeResult(s, rem) =>
        Try(parse(s)) match {
          case Success(a) => Attempt.successful(DecodeResult(wrap(a), rem))
          case Failure(e) => Attempt.failure(Err(s"ScodecDeriver: invalid $label (${e.getMessage})"))
        }
      }
  }

  // -------------------------------------------------------------------
  // Primitive
  // -------------------------------------------------------------------

  override def derivePrimitive[A](
    primitiveType: PrimitiveType[A],
    typeId:        TypeId[A],
    binding:       Binding.Primitive[A],
    doc:           Doc,
    modifiers:     Seq[Modifier.Reflect],
    defaultValue:  Option[A],
    examples:      Seq[A]
  ): Lazy[Codec[A]] = Lazy {
    primitiveType match {
      case PrimitiveType.Unit       => provide(()).asInstanceOf[Codec[A]]
      case _: PrimitiveType.Boolean => bool(8).asInstanceOf[Codec[A]]
      case _: PrimitiveType.Byte    => byte.asInstanceOf[Codec[A]]
      case _: PrimitiveType.Short   => short16.asInstanceOf[Codec[A]]
      case _: PrimitiveType.Int     => int32.asInstanceOf[Codec[A]]
      case _: PrimitiveType.Long    => int64.asInstanceOf[Codec[A]]
      case _: PrimitiveType.Float   => float.asInstanceOf[Codec[A]]
      case _: PrimitiveType.Double  => double.asInstanceOf[Codec[A]]
      case _: PrimitiveType.Char    => uint16.xmap[Char](_.toChar, _.toInt).asInstanceOf[Codec[A]]
      case _: PrimitiveType.String  => utf8_32.asInstanceOf[Codec[A]]
      case other                    =>
        unimplemented[A](s"primitive ${other.getClass.getSimpleName.stripSuffix("$")}")
    }
  }

  // -------------------------------------------------------------------
  // Record
  // -------------------------------------------------------------------

  override def deriveRecord[F[_, _], A](
    fields:        IndexedSeq[Term[F, A, ?]],
    typeId:        TypeId[A],
    binding:       Binding.Record[A],
    doc:           Doc,
    modifiers:     Seq[Modifier.Reflect],
    defaultValue:  Option[A],
    examples:      Seq[A]
  )(using F: HasBinding[F], D: SchemaHasInstance[F, Codec]): Lazy[Codec[A]] = {
    // Pre-compute structural setup outside the Lazy thunk so Lazy
    // body itself is cheap and side-effect free.
    val recordFields  = fields.asInstanceOf[IndexedSeq[Term[Binding, A, ?]]]
    val recordReflect = new Reflect.Record[Binding, A](recordFields, typeId, binding, doc, modifiers)
    val registerArr   = recordReflect.registers.toArray

    Lazy {
      // Defer child-instance resolution to first use - same recursive
      // safety pattern the Show example uses.
      lazy val resolvedCodecs: Array[Codec[Any]] =
        fields.iterator
          .map(field => D.instance(field.value.metadata).asInstanceOf[Lazy[Codec[Any]]].force)
          .toArray

      new Codec[A] {
        def sizeBound: SizeBound = SizeBound.unknown

        def encode(value: A): Attempt[BitVector] = {
          val regs = Registers(recordReflect.usedRegisters)
          binding.deconstructor.deconstruct(regs, RegisterOffset.Zero, value)
          var i   = 0
          var acc = BitVector.empty
          val n   = resolvedCodecs.length
          while (i < n) {
            val fieldValue = registerArr(i).get(regs, RegisterOffset.Zero)
            resolvedCodecs(i).encode(fieldValue) match {
              case Attempt.Successful(bits) => acc = acc ++ bits
              case f @ Attempt.Failure(_)   => return f
            }
            i += 1
          }
          Attempt.successful(acc)
        }

        def decode(bits: BitVector): Attempt[DecodeResult[A]] = {
          val regs = Registers(recordReflect.usedRegisters)
          var rem  = bits
          var i    = 0
          val n    = resolvedCodecs.length
          while (i < n) {
            resolvedCodecs(i).decode(rem) match {
              case Attempt.Successful(DecodeResult(v, r)) =>
                registerArr(i).set(regs, RegisterOffset.Zero, v)
                rem = r
              case f @ Attempt.Failure(_) => return f
            }
            i += 1
          }
          Attempt.successful(DecodeResult(binding.constructor.construct(regs, RegisterOffset.Zero), rem))
        }
      }
    }
  }

  // -------------------------------------------------------------------
  // Variant
  // -------------------------------------------------------------------

  override def deriveVariant[F[_, _], A](
    cases:        IndexedSeq[Term[F, A, ?]],
    typeId:       TypeId[A],
    binding:      Binding.Variant[A],
    doc:          Doc,
    modifiers:    Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples:     Seq[A]
  )(using F: HasBinding[F], D: SchemaHasInstance[F, Codec]): Lazy[Codec[A]] = {
    val caseLazies: IndexedSeq[Lazy[Codec[A]]] =
      cases.map(c => D.instance(c.value.metadata).asInstanceOf[Lazy[Codec[A]]])

    Lazy {
      lazy val resolved: Array[Codec[A]] = caseLazies.iterator.map(_.force).toArray
      val n = resolved.length
      require(n <= 256, s"ScodecDeriver: variant with more than 256 cases is not supported (got $n for ${typeId.name})")

      new Codec[A] {
        def sizeBound: SizeBound = SizeBound.unknown

        def encode(value: A): Attempt[BitVector] = {
          val idx = binding.discriminator.discriminate(value)
          val downcast = binding.matchers(idx).downcastOrNull(value).asInstanceOf[AnyRef]
          // `downcastOrNull` returns null when the discriminator is
          // wrong; comparing as AnyRef avoids the value-class trap.
          if (downcast eq null)
            Attempt.failure(Err(s"variant ${typeId.name}: failed to downcast case $idx"))
          else
            uint8.encode(idx).flatMap(tag =>
              resolved(idx).encode(downcast.asInstanceOf[A]).map(tag ++ _)
            )
        }

        def decode(bits: BitVector): Attempt[DecodeResult[A]] =
          uint8.decode(bits) match {
            case Attempt.Successful(DecodeResult(idx, rest)) =>
              if (idx < 0 || idx >= n)
                Attempt.failure(Err(s"variant ${typeId.name}: invalid tag $idx"))
              else
                resolved(idx).decode(rest)
            case f @ Attempt.Failure(_) => f
          }
      }
    }
  }

  // -------------------------------------------------------------------
  // Sequence
  // -------------------------------------------------------------------

  override def deriveSequence[F[_, _], C[_], A](
    element:      Reflect[F, A],
    typeId:       TypeId[C[A]],
    binding:      Binding.Seq[C, A],
    doc:          Doc,
    modifiers:    Seq[Modifier.Reflect],
    defaultValue: Option[C[A]],
    examples:     Seq[C[A]]
  )(using F: HasBinding[F], D: SchemaHasInstance[F, Codec]): Lazy[Codec[C[A]]] = {
    val constructor   = binding.constructor
    val deconstructor = binding.deconstructor
    val elemClassTag  = element.typeId.classTag.asInstanceOf[ClassTag[A]]

    /** `Seq[C]` / `List[C]` of `Byte`: one `int32` length + raw bytes (not N× `byte` codec). */
    if (isByteElement(elemClassTag)) {
      Lazy {
        new Codec[C[A]] {
          def sizeBound: SizeBound = SizeBound.unknown

          def encode(value: C[A]): Attempt[BitVector] = {
            val items = deconstructor.deconstruct(value).iterator
            val buf   = new scala.collection.mutable.ArrayBuffer[Byte]()
            while (items.hasNext) buf += items.next().asInstanceOf[Byte]
            val bytes = buf.toArray
            int32.encode(bytes.length).map(lenBits => lenBits ++ BitVector.view(bytes))
          }

          def decode(bits: BitVector): Attempt[DecodeResult[C[A]]] = {
            implicit val ct: ClassTag[A] = elemClassTag
            int32.decode(bits) match {
              case Attempt.Successful(DecodeResult(n, rest0)) =>
                if (n < 0)
                  Attempt.failure(Err(s"ScodecDeriver: negative sequence length $n"))
                else {
                  val needBits = n.toLong * 8L
                  if (rest0.size < needBits)
                    Attempt.failure(Err.InsufficientBits(needBits, rest0.size, Nil))
                  else {
                    val (payload, rem) = rest0.splitAt(needBits)
                    val bytes          = payload.toByteArray
                    if (bytes.length != n)
                      Attempt.failure(Err(s"ScodecDeriver: byte sequence size mismatch"))
                    else {
                      val builder = constructor.newBuilder[A](n)
                      var i       = 0
                      while (i < n) {
                        constructor.add(builder, bytes(i).asInstanceOf[A])
                        i += 1
                      }
                      Attempt.successful(DecodeResult(constructor.result(builder), rem))
                    }
                  }
                }
              case f @ Attempt.Failure(_) => f
            }
          }
        }
      }
    } else
      D.instance(element.metadata).map { elementCodec =>
        new Codec[C[A]] {
          def sizeBound: SizeBound = SizeBound.unknown

          def encode(value: C[A]): Attempt[BitVector] = {
            val items = deconstructor.deconstruct(value).iterator
            var acc   = BitVector.empty
            var count = 0
            // We don't know the size up-front for arbitrary C, so
            // encode into a temporary buffer and prepend the count.
            val buf = new java.util.ArrayList[BitVector]()
            while (items.hasNext) {
              elementCodec.encode(items.next()) match {
                case Attempt.Successful(b) => buf.add(b); count += 1
                case f @ Attempt.Failure(_)   => return f
              }
            }
            int32.encode(count).map { lenBits =>
              acc = lenBits
              var i = 0
              while (i < buf.size) { acc = acc ++ buf.get(i); i += 1 }
              acc
            }
          }

          def decode(bits: BitVector): Attempt[DecodeResult[C[A]]] = {
            implicit val ct: ClassTag[A] = elemClassTag
            int32.decode(bits) match {
              case Attempt.Successful(DecodeResult(n, rest0)) =>
                val builder = constructor.newBuilder[A](n)
                var rem     = rest0
                var i       = 0
                while (i < n) {
                  elementCodec.decode(rem) match {
                    case Attempt.Successful(DecodeResult(v, r)) =>
                      constructor.add(builder, v)
                      rem = r
                    case f @ Attempt.Failure(_) => return f
                  }
                  i += 1
                }
                Attempt.successful(DecodeResult(constructor.result(builder), rem))
              case f @ Attempt.Failure(_) => f
            }
          }
        }
      }
  }

  // -------------------------------------------------------------------
  // Map
  // -------------------------------------------------------------------

  override def deriveMap[F[_, _], M[_, _], K, V](
    key:          Reflect[F, K],
    value:        Reflect[F, V],
    typeId:       TypeId[M[K, V]],
    binding:      Binding.Map[M, K, V],
    doc:          Doc,
    modifiers:    Seq[Modifier.Reflect],
    defaultValue: Option[M[K, V]],
    examples:     Seq[M[K, V]]
  )(using F: HasBinding[F], D: SchemaHasInstance[F, Codec]): Lazy[Codec[M[K, V]]] = {
    val constructor   = binding.constructor
    val deconstructor = binding.deconstructor

    D.instance(key.metadata).zip(D.instance(value.metadata)).map { case (kCodec, vCodec) =>
      new Codec[M[K, V]] {
        def sizeBound: SizeBound = SizeBound.unknown

        def encode(m: M[K, V]): Attempt[BitVector] = {
          val items = deconstructor.deconstruct(m).iterator
          val buf   = new java.util.ArrayList[BitVector]()
          var count = 0
          while (items.hasNext) {
            val kv = items.next()
            val k  = deconstructor.getKey(kv)
            val v  = deconstructor.getValue(kv)
            kCodec.encode(k) match {
              case Attempt.Successful(kb) =>
                vCodec.encode(v) match {
                  case Attempt.Successful(vb) => buf.add(kb ++ vb); count += 1
                  case f @ Attempt.Failure(_) => return f
                }
              case f @ Attempt.Failure(_) => return f
            }
          }
          int32.encode(count).map { lenBits =>
            var acc = lenBits
            var i   = 0
            while (i < buf.size) { acc = acc ++ buf.get(i); i += 1 }
            acc
          }
        }

        def decode(bits: BitVector): Attempt[DecodeResult[M[K, V]]] = {
          int32.decode(bits) match {
            case Attempt.Successful(DecodeResult(n, rest0)) =>
              val builder = constructor.newObjectBuilder[K, V](n)
              var rem     = rest0
              var i       = 0
              while (i < n) {
                kCodec.decode(rem) match {
                  case Attempt.Successful(DecodeResult(k, r1)) =>
                    vCodec.decode(r1) match {
                      case Attempt.Successful(DecodeResult(v, r2)) =>
                        constructor.addObject(builder, k, v)
                        rem = r2
                      case f @ Attempt.Failure(_) => return f
                    }
                  case f @ Attempt.Failure(_) => return f
                }
                i += 1
              }
              Attempt.successful(DecodeResult(constructor.resultObject(builder), rem))
            case f @ Attempt.Failure(_) => f
          }
        }
      }
    }
  }

  // -------------------------------------------------------------------
  // Wrapper
  // -------------------------------------------------------------------

  override def deriveWrapper[F[_, _], A, B](
    wrapped:      Reflect[F, B],
    typeId:       TypeId[A],
    binding:      Binding.Wrapper[A, B],
    doc:          Doc,
    modifiers:    Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples:     Seq[A]
  )(using F: HasBinding[F], D: SchemaHasInstance[F, Codec]): Lazy[Codec[A]] =
    D.instance(wrapped.metadata).map { inner =>
      inner.xmap[A](b => binding.wrap(b), a => binding.unwrap(a))
    }

  // -------------------------------------------------------------------
  // Dynamic (Schema[DynamicValue] → recursive binary tree codec)
  // -------------------------------------------------------------------

  override def deriveDynamic[F[_, _]](
    binding:      Binding.Dynamic,
    doc:          Doc,
    modifiers:    Seq[Modifier.Reflect],
    defaultValue: Option[DynamicValue],
    examples:     Seq[DynamicValue]
  )(using F: HasBinding[F], D: SchemaHasInstance[F, Codec]): Lazy[Codec[DynamicValue]] =
    Lazy(dynamicValueCodec)

  // -------------------------------------------------------------------
  // helpers
  // -------------------------------------------------------------------

  private def isByteElement(ct: ClassTag[?]): Boolean = {
    val c = ct.runtimeClass
    c == java.lang.Byte.TYPE || c == classOf[java.lang.Byte]
  }

  private def unimplemented[A](what: String): Codec[A] = new Codec[A] {
    def sizeBound: SizeBound                   = SizeBound.unknown
    def encode(a: A): Attempt[BitVector]       =
      Attempt.failure(Err(s"ScodecDeriver: encoding not implemented for $what"))
    def decode(b: BitVector): Attempt[DecodeResult[A]] =
      Attempt.failure(Err(s"ScodecDeriver: decoding not implemented for $what"))
  }
}
