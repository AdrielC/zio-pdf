package zio.pdf

import zio.Chunk

/** Platform-local raw-byte digest state used by the one-pass evidence runner. */
private[pdf] trait EvidenceDigest:
  def update(chunk: Chunk[Byte]): Unit
  def finish(): Chunk[Byte]

private[pdf] object EvidenceDigest:
  def create(): EvidenceDigest =
    new EvidenceDigest:
      private val state = NobleSha256.create()

      def update(chunk: Chunk[Byte]): Unit =
        DigestChunk.update(state, chunk)

      def finish(): Chunk[Byte] =
        Chunk.fromArray(JsBinary.bytes(state.digest()))

private[pdf] object DigestChunk:
  private val MaxStepBytes = 64 * 1024

  def update(state: NobleSha256State, chunk: Chunk[Byte]): Unit =
    var from = 0
    while from < chunk.size do
      val length = math.min(MaxStepBytes, chunk.size - from)
      state.update(JsBinary.uint8(chunk, from, length))
      from += length
