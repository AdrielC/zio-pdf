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

import zio.blocks.docs.Doc
import zio.blocks.schema.{DynamicValue, Lazy, Modifier, PrimitiveType, Reflect, Term}
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
 *   - Dynamic: not implemented in this prototype - calling the
 *     derived codec for a `DynamicValue` will fail at decode time.
 */
object ScodecDeriver extends Deriver[Codec] {

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
  // Dynamic - not supported by this prototype
  // -------------------------------------------------------------------

  override def deriveDynamic[F[_, _]](
    binding:      Binding.Dynamic,
    doc:          Doc,
    modifiers:    Seq[Modifier.Reflect],
    defaultValue: Option[DynamicValue],
    examples:     Seq[DynamicValue]
  )(using F: HasBinding[F], D: SchemaHasInstance[F, Codec]): Lazy[Codec[DynamicValue]] =
    Lazy(unimplemented[DynamicValue]("DynamicValue"))

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
