/*
 * Helper data type for encoding streams of indirect objects.
 *
 * `Obj`         - a fully-materialised IndirectObj. Use for normal,
 *                 small / medium-sized objects.
 * `StreamObj`   - an indirect object whose stream payload is a
 *                 `ZStream[Any, Throwable, Byte]`. Use for large
 *                 attachments / images / fonts where materialising
 *                 the whole payload would blow the heap.
 *                 The encoder writes the object header + dict +
 *                 `stream\n` keyword, then forwards the byte stream
 *                 chunk-by-chunk, then writes `\nendstream\nendobj`,
 *                 never holding the payload in memory.
 *
 *                 The dict's `/Length` MUST be set correctly by the
 *                 caller - the encoder doesn't know the length up
 *                 front. Pass it as `length: Long`; the encoder
 *                 will splice it into the dict before encoding.
 *                 If the supplied stream produces a different number
 *                 of bytes the encoder will fail at end-of-stream.
 * `Meta`        - trailer / metadata side-channel.
 * `Version`     - PDF version header (must come first if present).
 */

package zio.pdf

import zio.stream.ZStream

sealed trait Part[+A]

object Part {

  /** A fully-materialised indirect object. */
  final case class Obj(obj: IndirectObj) extends Part[Nothing]

  /**
   * An indirect object with a streaming payload. `length` is the
   * exact byte count of `payload`; it is patched into `data` as
   * `/Length <length>` before encoding the dict.
   */
  final case class StreamObj(
    index: zio.pdf.Obj.Index,
    data: Prim,
    length: Long,
    payload: ZStream[Any, Throwable, Byte]
  ) extends Part[Nothing]

  final case class Meta[A](meta: A) extends Part[A]

  final case class Version(version: zio.pdf.Version) extends Part[Nothing]
}
