package zio.pdf

import zio.Chunk
import zio.stream.ZStream

/**
 * A bounded, streaming LCS alignment.
 *
 * A global longest-common-subsequence requires retaining both complete input
 * sequences. This implementation instead aligns lockstep windows, keeping at
 * most `windowSize` elements from each input in the LCS table. Each emitted
 * [[Window]] is an exact LCS edit script for its two windows; callers that
 * require a globally minimal edit script must explicitly collect their input
 * before calling an offline algorithm.
 */
object StreamingLcs:

  final class Config private (val windowSize: Int, val maximumCells: Long):
    private val tableCells = (windowSize.toLong + 1L) * (windowSize.toLong + 1L)

    require(windowSize > 0, "windowSize must be positive")
    require(tableCells <= Int.MaxValue.toLong, "windowSize is too large for an in-memory LCS table")
    require(maximumCells >= tableCells, "maximumCells must fit one LCS window including its boundary cells")

  object Config:
    def apply(windowSize: Int = 128, maximumCells: Long = 0L): Config =
      val tableCells = (windowSize.toLong + 1L) * (windowSize.toLong + 1L)
      new Config(windowSize, if maximumCells == 0L then tableCells else maximumCells)

  enum Edit[+A]:
    case Same(left: A, right: A)
    case Removed(left: A)
    case Added(right: A)

  final case class Window[+A](
    index: Long,
    leftSize: Int,
    rightSize: Int,
    edits: Chunk[Edit[A]]
  ):
    /** The edit script is exact for this bounded pair of input windows. */
    val exactWithinWindow: Boolean = true

  /**
   * Align two streams using bounded lockstep windows.
   *
   * `ZStream.grouped` is the only buffer here. It preserves back-pressure and
   * never collects either PDF (or any other input) in full.
   */
  def windows[R1, R2, E, A](
    left: ZStream[R1, E, A],
    right: ZStream[R2, E, A],
    config: Config = Config()
  )(
    equivalent: (A, A) => Boolean
  ): ZStream[R1 & R2, E, Window[A]] =
    left
      .grouped(config.windowSize)
      .zipAll(right.grouped(config.windowSize))(Chunk.empty, Chunk.empty)
      .zipWithIndex
      .map { case ((leftWindow, rightWindow), index) =>
        Window(index, leftWindow.size, rightWindow.size, align(leftWindow, rightWindow)(equivalent))
      }

  private def align[A](left: Chunk[A], right: Chunk[A])(equivalent: (A, A) => Boolean): Chunk[Edit[A]] =
    val rows  = left.size + 1
    val cols  = right.size + 1
    val table = new Array[Int](rows * cols)

    var leftIndex = left.size - 1
    while leftIndex >= 0 do
      var rightIndex = right.size - 1
      while rightIndex >= 0 do
        val cell = leftIndex * cols + rightIndex
        table(cell) =
          if equivalent(left(leftIndex), right(rightIndex)) then
            table((leftIndex + 1) * cols + rightIndex + 1) + 1
          else
            math.max(table((leftIndex + 1) * cols + rightIndex), table(leftIndex * cols + rightIndex + 1))
        rightIndex -= 1
      leftIndex -= 1

    val builder = Chunk.newBuilder[Edit[A]]
    var i       = 0
    var j       = 0
    while i < left.size && j < right.size do
      if equivalent(left(i), right(j)) then
        builder += Edit.Same(left(i), right(j))
        i += 1
        j += 1
      else if table((i + 1) * cols + j) >= table(i * cols + j + 1) then
        builder += Edit.Removed(left(i))
        i += 1
      else
        builder += Edit.Added(right(j))
        j += 1

    while i < left.size do
      builder += Edit.Removed(left(i))
      i += 1
    while j < right.size do
      builder += Edit.Added(right(j))
      j += 1
    builder.result()
