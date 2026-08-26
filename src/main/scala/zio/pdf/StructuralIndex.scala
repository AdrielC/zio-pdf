/*
 * Conservative, xref-backed parallel expansion for already-owned PDF bytes.
 *
 * The normal fused decoder is the universal parser. This optimization only
 * accepts conventional and xref-stream revisions whose object ranges can be
 * independently decoded. Any uncertainty returns None and the caller uses the
 * universal decoder unchanged.
 */

package zio.pdf

import _root_.scodec.{Attempt, Codec, DecodeResult}
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.{Chunk, NonEmptyChunk, Task, ZIO}

import scala.collection.mutable.ArrayBuffer

private[pdf] object StructuralIndex {

  /** Skip scheduler/index overhead for small, already-fast in-memory PDFs. */
  val MinimumParallelBytes: Int = 1024 * 1024
  private val WorkGroupBytes = 512 * 1024

  private[pdf] final case class ObjectRef(number: Long, generation: Int, offset: Int)
  private[pdf] final case class ObjectRange(ref: ObjectRef, end: Int) {
    def size: Int = end - ref.offset
  }

  private[pdf] enum XrefSource {
    case Text, Stream
  }

  private[pdf] final case class IndexedXref(
    source: XrefSource,
    offset: Int,
    previousOffset: Option[Int],
    value: Xref
  )

  private[pdf] final case class Indexed(
    version: Version,
    xrefs: Chunk[IndexedXref],
    private val ranges: Chunk[ObjectRange]
  ) {

    /**
     * Preserve physical object order while giving workers similarly sized
     * contiguous groups. A group never splits an indirect object.
     */
    private def groups: Chunk[Chunk[ObjectRange]] = {
      val result = Chunk.newBuilder[Chunk[ObjectRange]]
      val group  = Chunk.newBuilder[ObjectRange]
      var bytes  = 0L
      val it     = ranges.iterator

      while it.hasNext do {
        val range = it.next()
        if bytes > 0L && bytes + range.size > WorkGroupBytes then {
          result += group.result()
          group.clear()
          bytes = 0L
        }
        group += range
        bytes += range.size.toLong
      }
      if bytes > 0L then result += group.result()
      result.result()
    }

    private def meta: Decoded.Meta = {
      val textXrefs   = xrefs.collect { case IndexedXref(XrefSource.Text, _, _, xref)   => xref }
      val streamXrefs = xrefs.collect { case IndexedXref(XrefSource.Stream, _, _, xref) => xref }
      val all          = textXrefs.toList ++ streamXrefs.toList
      val trailers     = NonEmptyChunk.fromIterableOption(all.map(_.trailer))
      Decoded.Meta(all, trailers.map(Trailer.sanitize), Some(version))
    }

    /** Decode canonical object ranges in parallel, then restore byte order. */
    def decode(bytes: Array[Byte]): Task[Chunk[Decoded]] =
      // foreachPar inherits ZIO's scoped parallelism FiberRef. Callers can
      // write `PdfEngine.decode(bytes).withParallelism(n)` without a PDF knob.
      ZIO
        .foreachPar(groups)(group => ZIO.attempt(decodeGroup(bytes, group)))
        .map { decodedGroups =>
          decodedGroups.foldLeft(Chunk.empty[Decoded])(_ ++ _) ++ Chunk.single(meta)
        }
  }

  /** Every accepted xref is physically present in the input and independently parsed. */
  def index(bytes: Array[Byte]): Option[Indexed] =
    for {
      version    <- decodeAt[Version](bytes, 0)(using Version.codec)
      xrefs      <- allXrefs(bytes)
      // Repeated textual xrefs describe a current object view, while the
      // fused decoder intentionally preserves every physical object revision.
      // Xref streams have already proved that property in the corpus; retain
      // the stricter fallback for incremental textual-xref files.
      _          <- Option.when(xrefs.count(_.source == XrefSource.Text) <= 1) (())
      objectRefs <- refs(xrefs, bytes.length)
      ranges     <- rangesFor(objectRefs, bytes.length)
    } yield Indexed(version, xrefs, ranges)

  private def decodeGroup(bytes: Array[Byte], group: Chunk[ObjectRange]): Chunk[Decoded] = {
    val builder = Chunk.newBuilder[Decoded]
    val it      = group.iterator
    while it.hasNext do {
      val range = it.next()
      decodeObject(bytes, range).foreach(builder += _)
    }
    builder.result()
  }

  private def decodeObject(bytes: Array[Byte], range: ObjectRange): Chunk[Decoded] = {
    // `ByteVector.view` retains the caller-owned byte array instead of copying
    // every indexed object range before handing it to scodec.
    val objectBits = BitVector(ByteVector.view(bytes, range.ref.offset, range.size))
    val indirect = summon[Codec[IndirectObj]].decode(objectBits) match {
      case Attempt.Successful(DecodeResult(value, _)) => value
      case Attempt.Failure(cause) =>
        throw new RuntimeException(s"xref object at ${range.ref.offset}: ${cause.messageWithContext}")
    }

    if indirect.obj.index.number != range.ref.number ||
        indirect.obj.index.generation != range.ref.generation then
      throw new IllegalStateException(s"xref object at ${range.ref.offset} has the wrong object identity")

    indirect match {
      case IndirectObj(obj, None) =>
        Chunk.single(Decoded.DataObj(obj))
      case IndirectObj(Obj(_, Prim.tpe("XRef", _)), Some(_)) =>
        // Metadata was decoded while indexing every startxref revision. Xref
        // streams have no Decoded row of their own.
        Chunk.empty
      case IndirectObj(Obj(index, data), Some(rawStream)) =>
        Decode.expandStreamPayload(index, data, rawStream) match {
          case Attempt.Successful(Right(decoded)) => Chunk.fromIterable(decoded)
          case Attempt.Successful(Left(_)) =>
            throw new IllegalStateException("unindexed xref stream")
          case Attempt.Failure(cause) =>
            throw new RuntimeException(s"expand xref object at ${range.ref.offset}: ${cause.messageWithContext}")
        }
    }
  }

  private def allXrefs(bytes: Array[Byte]): Option[Chunk[IndexedXref]] = {
    val markers = allIndicesOf(bytes, "startxref".getBytes("US-ASCII"))
    val result  = scala.collection.mutable.Map.empty[Int, IndexedXref]
    val it      = markers.iterator

    while it.hasNext do {
      val marker = it.next()
      val root = for {
        start  <- decodeAt[StartXref](bytes, marker)(using StartXref.codec)
        offset <- if start.offset >= 0L && start.offset <= Int.MaxValue then Some(start.offset.toInt) else None
        _      <- Option.when(offset < marker)(())
      } yield offset
      root match {
        // PDF linearization permits an early `startxref 0` marker before the
        // main body. It does not point at a structural xref.
        case Some(0) => ()
        case Some(offset) if !collectXrefs(bytes, offset, result) => return None
        case Some(_) | None => ()
      }
    }

    if result.isEmpty then None
    else {
      val ordered = result.valuesIterator.toList.sortBy(_.offset)
      Option.when(ordered.nonEmpty)(Chunk.fromIterable(ordered))
    }
  }

  private def collectXrefs(
    bytes: Array[Byte],
    offset: Int,
    result: scala.collection.mutable.Map[Int, IndexedXref]
  ): Boolean =
    if result.contains(offset) then true
    else
      xrefAt(bytes, offset) match {
        case Some(xref) =>
          result += offset -> xref
          xref.previousOffset.forall(previous => collectXrefs(bytes, previous, result))
        case None => false
      }

  private def xrefAt(bytes: Array[Byte], offset: Int): Option[IndexedXref] =
    decodeAt[Xref](bytes, offset)(using summon[Codec[Xref]])
      .map(xref => IndexedXref(XrefSource.Text, offset, previousOffset(xref.trailer.data), xref))
      .orElse {
        decodeAt[IndirectObj](bytes, offset)(using summon[Codec[IndirectObj]]).flatMap {
          case IndirectObj(Obj(_, data @ Prim.tpe("XRef", dict)), Some(rawStream)) =>
            Content.uncompress(rawStream)(data).exec match {
              case Attempt.Successful(bits) =>
                XrefStream(dict)(bits) match {
                  case Attempt.Successful(stream) =>
                    Some(
                      IndexedXref(
                        XrefSource.Stream,
                        offset,
                        previousOffset(dict),
                        Xref(stream.tables, stream.trailer, StartXref(0L))
                      )
                    )
                  case Attempt.Failure(_) => None
                }
              case Attempt.Failure(_) => None
            }
          case _ => None
        }
      }

  private def previousOffset(data: Prim.Dict): Option[Int] =
    data.data.get("Prev").collect { case Prim.Number(offset) if offset.isValidInt && offset >= 0 => offset.toInt }

  private def refs(xrefs: Chunk[IndexedXref], length: Int): Option[Chunk[ObjectRef]] = {
    val result = ArrayBuffer.empty[ObjectRef]
    val seen   = scala.collection.mutable.HashSet.empty[(Long, Int, Int)]
    val sources = xrefs.iterator

    while sources.hasNext do {
      val tables = sources.next().value.tables.iterator
      while tables.hasNext do {
        val table = tables.next()
        var number = table.offset
        val entries = table.entries.iterator
        while entries.hasNext do {
          entries.next() match {
            case Xref.Entry(Xref.Index.Regular(offset, generation), Xref.EntryType.InUse) =>
              val parsed = for {
                byteOffset <- offset.toIntOption
                gen        <- generation.toIntOption
                _          <- Option.when(byteOffset > 0 && byteOffset < length)(())
                _          <- Option.when(seen.add((number, gen, byteOffset)))(())
              } yield ObjectRef(number, gen, byteOffset)
              parsed match {
                case Some(ref) => result += ref
                case None      => return None
              }
            case Xref.Entry(_: Xref.Index.Regular, Xref.EntryType.Free) => ()
            case Xref.Entry(_: Xref.Index.Compressed, Xref.EntryType.InUse) => ()
            case _: Xref.Entry => return None
          }
          number += 1L
        }
      }
    }

    Option.when(result.nonEmpty)(Chunk.fromIterable(result.sortBy(_.offset)))
  }

  private def rangesFor(refs: Chunk[ObjectRef], endOfInput: Int): Option[Chunk[ObjectRange]] = {
    val builder = Chunk.newBuilder[ObjectRange]
    var previous = -1
    var index    = 0
    while index < refs.size do {
      val ref = refs(index)
      val end = if index + 1 < refs.size then refs(index + 1).offset else endOfInput
      if ref.offset <= previous || end <= ref.offset then return None
      builder += ObjectRange(ref, end)
      previous = ref.offset
      index += 1
    }
    Some(builder.result())
  }

  private def decodeAt[A](bytes: Array[Byte], offset: Int)(using codec: Codec[A]): Option[A] =
    if offset < 0 || offset >= bytes.length then None
    else
      codec.decode(BitVector(ByteVector.view(bytes, offset, bytes.length - offset))) match {
        case Attempt.Successful(DecodeResult(value, _)) => Some(value)
        case Attempt.Failure(_)                         => None
      }

  private def allIndicesOf(bytes: Array[Byte], needle: Array[Byte]): Chunk[Int] = {
    val result = Chunk.newBuilder[Int]
    var at     = 0
    while at <= bytes.length - needle.length do {
      var offset = 0
      while offset < needle.length && bytes(at + offset) == needle(offset) do offset += 1
      if offset == needle.length then {
        result += at
        at += needle.length
      } else at += 1
    }
    result.result()
  }
}
