package zio.pdf

import _root_.scodec.bits.BitVector
import zio.Chunk
import zio.blocks.schema.{DynamicValue, Schema}
import zio.blocks.schema.patch.DynamicPatch
import zio.stream.ZStream

/**
 * Schema-aware, streaming semantic PDF comparison.
 *
 * Decoder events are projected into [[Component]] values before alignment.
 * Their values are compared as `DynamicValue`. Content streams contribute a
 * compact schema-visible fingerprint by default; callers can opt into raw-byte
 * verification for every stream match. Object numbers are evidence locations,
 * not semantic identity, so equivalent objects can still align after a rewrite
 * renumbers them.
 */
object PdfDiff:

  final case class ObjectRef(number: Long, generation: Int)

  object ObjectRef:
    given Schema[ObjectRef] = Schema.derived[ObjectRef]

  enum Location:
    case Object(ref: ObjectRef)
    case Document

  object Location:
    given Schema[Location] = Schema.derived[Location]

  final case class StreamFingerprint(byteLength: Long, rollingHash: Long)

  object StreamFingerprint:
    given Schema[StreamFingerprint] = Schema.derived[StreamFingerprint]

  final case class VersionInfo(major: Int, minor: Int)

  object VersionInfo:
    given Schema[VersionInfo] = Schema.derived[VersionInfo]

  enum Value:
    case Primitive(value: Prim)
    case Stream(dictionary: Prim, payload: StreamFingerprint)
    case Metadata(trailer: Option[Prim], version: Option[VersionInfo], xrefCount: Int)

  object Value:
    given Schema[Value] = Schema.derived[Value]

  /**
   * A schema-visible PDF component. `location` gives the source object or
   * document scope; `value` is the semantic projection used by LCS matching.
   */
  final case class Component(location: Location, value: Value)

  object Component:
    given Schema[Component] = Schema.derived[Component]

  /** A semantic edit with schema-level detail for replacements. */
  enum Edit:
    case Same(left: Component, right: Component)
    case Removed(left: Component)
    case Added(right: Component)
    case Changed(left: Component, right: Component, patch: DynamicPatch, payloadChanged: Boolean)

  /** One exact LCS alignment over a bounded, lockstep pair of input windows. */
  final case class Window(index: Long, leftSize: Int, rightSize: Int, edits: Chunk[Edit]):
    val exactWithinWindow: Boolean = true

  final class Config private (
    val windowSize: Int,
    val maximumCells: Long,
    val verifyRawStreamPayloads: Boolean
  ):
    private[pdf] val lcs: StreamingLcs.Config = StreamingLcs.Config(windowSize, maximumCells)

  object Config:
    val default: Config = Config()

    def apply(
      windowSize: Int = 128,
      maximumCells: Long = 0L,
      verifyRawStreamPayloads: Boolean = false
    ): Config =
      val tableCells = (windowSize.toLong + 1L) * (windowSize.toLong + 1L)
      new Config(
        windowSize,
        if maximumCells == 0L then tableCells else maximumCells,
        verifyRawStreamPayloads
      )

  /**
   * Project and compare two decoded PDF streams without assembling either PDF.
   *
   * The result is exact for the configured semantic component projection in
   * each LCS window. `verifyRawStreamPayloads` upgrades content-stream matches
   * to exact byte equality, retaining each window's raw stream payloads for
   * that verification. A globally minimal LCS would require retaining whole
   * documents, so this deliberately does not claim to be a global diff.
   */
  def fromDecoded[R1, R2](
    left: ZStream[R1, Throwable, Decoded],
    right: ZStream[R2, Throwable, Decoded],
    config: Config = Config.default
  ): ZStream[R1 & R2, Throwable, Window] =
    StreamingLcs
      .windows(
        left.map(project(_, config.verifyRawStreamPayloads)),
        right.map(project(_, config.verifyRawStreamPayloads)),
        config.lcs
      )(equivalent)
      .map(window => Window(window.index, window.leftSize, window.rightSize, edits(window.edits)))

  /** The dynamic, schema-derived semantic projection used for matching and patches. */
  def dynamicValue(component: Component): DynamicValue =
    summon[Schema[Value]].toDynamicValue(component.value)

  /** The whole schema-derived evidence component, including its source location. */
  def componentDynamicValue(component: Component): DynamicValue =
    summon[Schema[Component]].toDynamicValue(component)

  private final case class Candidate(component: Component, dynamic: DynamicValue, rawPayload: Option[BitVector])

  private def project(decoded: Decoded, retainRawPayload: Boolean): Candidate =
    decoded match
      case Decoded.DataObj(Obj(index, data)) =>
        candidate(Component(Location.Object(ObjectRef(index.number, index.generation)), Value.Primitive(data)))
      case Decoded.ContentObj(Obj(index, dictionary), rawStream, _) =>
        val fingerprint = streamFingerprint(rawStream)
        Candidate(
          Component(Location.Object(ObjectRef(index.number, index.generation)), Value.Stream(dictionary, fingerprint)),
          summon[Schema[Value]].toDynamicValue(Value.Stream(dictionary, fingerprint)),
          Option.when(retainRawPayload)(rawStream)
        )
      case Decoded.Meta(xrefs, trailer, version) =>
        val value = Value.Metadata(
          trailer.map(_.data),
          version.map(value => VersionInfo(value.major, value.minor)),
          xrefs.size
        )
        candidate(Component(Location.Document, value))

  private def candidate(component: Component): Candidate =
    Candidate(component, dynamicValue(component), None)

  private def equivalent(left: Candidate, right: Candidate): Boolean =
    left.dynamic == right.dynamic &&
      ((left.rawPayload, right.rawPayload) match
        case (Some(leftBytes), Some(rightBytes)) => leftBytes == rightBytes
        case (None, None)                        => true
        case _                                    => false)

  private def edits(alignment: Chunk[StreamingLcs.Edit[Candidate]]): Chunk[Edit] =
    val builder = Chunk.newBuilder[Edit]
    var index   = 0
    while index < alignment.size do
      alignment(index) match
        case StreamingLcs.Edit.Same(left, right) =>
          builder += Edit.Same(left.component, right.component)
          index += 1
        case StreamingLcs.Edit.Removed(_) =>
          val removed           = Chunk.newBuilder[Candidate]
          var readingRemovals   = true
          while index < alignment.size && readingRemovals do
            alignment(index) match
              case StreamingLcs.Edit.Removed(left) =>
                removed += left
                index += 1
              case _ =>
                readingRemovals = false
          val removedComponents = removed.result()

          val added         = Chunk.newBuilder[Candidate]
          var readingAddeds = true
          while index < alignment.size && readingAddeds do
            alignment(index) match
              case StreamingLcs.Edit.Added(right) =>
                added += right
                index += 1
              case _ =>
                readingAddeds = false
          val addedComponents = added.result()

          val replacements = math.min(removedComponents.size, addedComponents.size)
          var replacement  = 0
          while replacement < replacements do
            val left  = removedComponents(replacement)
            val right = addedComponents(replacement)
            builder += Edit.Changed(
              left.component,
              right.component,
              left.dynamic.diff(right.dynamic),
              payloadChanged(left.component.value, right.component.value)
            )
            replacement += 1
          while replacement < removedComponents.size do
            builder += Edit.Removed(removedComponents(replacement).component)
            replacement += 1
          var addedIndex = replacements
          while addedIndex < addedComponents.size do
            builder += Edit.Added(addedComponents(addedIndex).component)
            addedIndex += 1
        case StreamingLcs.Edit.Added(right) =>
          builder += Edit.Added(right.component)
          index += 1
    builder.result()

  private def streamFingerprint(stream: BitVector): StreamFingerprint =
    val rollingHash = stream.bytes.foldLeft(-3750763034362895579L) { (hash, byte) =>
      (hash ^ (byte.toLong & 0xffL)) * 1099511628211L
    }
    StreamFingerprint(stream.bytes.size.toLong, rollingHash)

  private def payloadChanged(left: Value, right: Value): Boolean =
    (left, right) match
      case (Value.Stream(_, leftPayload), Value.Stream(_, rightPayload)) => leftPayload != rightPayload
      case _                                                              => false
