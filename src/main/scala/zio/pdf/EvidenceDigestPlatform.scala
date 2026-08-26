package zio.pdf

import java.security.MessageDigest

import zio.Chunk

/** Platform-local raw-byte digest state used by the one-pass evidence runner. */
private[pdf] trait EvidenceDigest:
  def update(chunk: Chunk[Byte]): Unit
  def finish(): Chunk[Byte]

private[pdf] object EvidenceDigest:
  def create(): EvidenceDigest =
    new EvidenceDigest:
      private val state = MessageDigest.getInstance("SHA-256")

      def update(chunk: Chunk[Byte]): Unit =
        DigestChunk.update(state, chunk)

      def finish(): Chunk[Byte] = Chunk.fromArray(state.digest())

private[pdf] object DigestChunk:
  private val MaxStepBytes = 64 * 1024

  def update(state: MessageDigest, chunk: Chunk[Byte]): Unit =
    chunk match
      case Chunk.ByteArray(bytes, offset, length) => state.update(bytes, offset, length)
      case _ =>
        var from = 0
        while from < chunk.size do
          val length = math.min(MaxStepBytes, chunk.size - from)
          val bytes  = new Array[Byte](length)
          var index  = 0
          while index < length do
            bytes(index) = chunk(from + index)
            index += 1
          state.update(bytes, 0, length)
          from += length
