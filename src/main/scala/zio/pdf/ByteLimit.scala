package zio.pdf

/**
 * A positive byte bound that is representable by JVM and JavaScript arrays.
 * APIs which materialize bytes require this type instead of an unbounded Long.
 */
final case class ByteLimit private (bytes: Int):
  def toLong: Long = bytes.toLong

object ByteLimit:
  final case class Invalid(value: Long)
      extends IllegalArgumentException(s"byte materialization limit must be between 1 and ${Int.MaxValue}: $value")

  def fromBytes(value: Long): Either[Invalid, ByteLimit] =
    if value > 0L && value <= Int.MaxValue.toLong then Right(ByteLimit(value.toInt))
    else Left(Invalid(value))

  def mebibytes(value: Int): ByteLimit =
    fromBytes(Math.multiplyExact(value.toLong, 1024L * 1024L)).fold(throw _, identity)

  val DefaultStreamMaterialization: ByteLimit = mebibytes(64)
  val DefaultReadAll: ByteLimit               = mebibytes(64)
  val DefaultDocumentMaterialization: ByteLimit = mebibytes(256)
