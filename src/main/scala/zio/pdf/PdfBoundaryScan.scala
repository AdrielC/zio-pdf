/*
 * Tight object-boundary scan on a byte cursor — no scodec, no BitVector,
 * no StreamingDecode. Parser state lives in a flat register block plus one
 * reusable carry buffer (grow-only, no per-step merge alloc).
 */

package zio.pdf

import zio.Chunk

private[pdf] object PdfBoundaryScan {

  private val pdfMagic    = asciiBytes"%PDF-"
  private val streamKw    = asciiBytes"stream"
  private val objKw       = asciiBytes"obj"
  private val endobjKw    = asciiBytes"endobj"
  private val endstreamKw = asciiBytes"endstream"
  private val xrefKw      = asciiBytes"xref"
  private val eofKw       = asciiBytes"%%EOF"
  private val lengthName  = asciiBytes"Length"
  private val startxrefKw = asciiBytes"startxref"

  private inline val PhaseHeader        = 0
  private inline val PhaseSkipPayload   = 1
  private inline val PhaseStreamTrailer = 2

  private inline val InitialCarryCap = 512

  /** Mutable scan registers — no case-class copies on the hot path. */
  final class Machine private (
    val dup: DuplicateFilterState.Mutable,
    private[pdf] var carryBuf: Array[Byte],
    var carryLen: Int,
    var bytesSeen: Long,
    var phase: Int,
    var objNum: Long,
    var objGen: Int,
    var remaining: Long,
    var emitBoundary: Boolean,
    /** Object header already committed for duplicate filtering (survives carry re-parse). */
    var inFlightNum: Long,
    var inFlightGen: Int
  ) {
    private[pdf] def clearInFlight(): Unit =
      inFlightNum = -1L
      inFlightGen = -1

    private[pdf] def commitObjectHeader(number: Long, generation: Int): Unit =
      if inFlightNum != number || inFlightGen != generation then
        inFlightNum = number
        inFlightGen = generation
        objNum = number
        objGen = generation
        emitBoundary = !DuplicateFilterState.shouldSuppress(dup, number)
    private[pdf] def ensureCarryCapacity(need: Int): Unit =
      if need <= carryBuf.length then ()
      else
        var cap = carryBuf.length.max(InitialCarryCap)
        while cap < need do cap *= 2
        carryBuf = java.util.Arrays.copyOf(carryBuf, cap)

    private[pdf] def retainWork(work: Array[Byte], from: Int, end: Int): Unit =
      val hold = end - from
      ensureCarryCapacity(hold)
      System.arraycopy(work, from, carryBuf, 0, hold)
      carryLen = hold

    private[pdf] def mergeCarry(buf: Array[Byte], offset: Int, length: Int): (Array[Byte], Int, Int) =
      val old = carryLen
      ensureCarryCapacity(old + length)
      System.arraycopy(carryBuf, 0, carryBuf, 0, old)
      System.arraycopy(buf, offset, carryBuf, old, length)
      carryLen = 0
      (carryBuf, 0, old + length)
  }

  object Machine {
    def fresh(): Machine =
      new Machine(
        DuplicateFilterState.initial,
        new Array[Byte](InitialCarryCap),
        0,
        0L,
        PhaseHeader,
        0L,
        0,
        0L,
        emitBoundary = true,
        inFlightNum = -1L,
        inFlightGen = -1
      )
  }

  /** Opaque cursor handle — the [[Machine]] is mutated in place across steps. */
  final class State private[pdf] (private[pdf] val machine: Machine)

  def initial: State = new State(Machine.fresh())

  def withBytesSeen(seen: Long): State =
    val m = Machine.fresh()
    m.bytesSeen = seen
    new State(m)

  def validate(m: Machine): Either[StreamingDecode.UnexpectedEndOfInput, Unit] =
    m.phase match
      case PhaseHeader if m.carryLen == 0 => Right(())
      case PhaseHeader if carryIsTriviaOnly(m) =>
        m.carryLen = 0
        Right(())
      case PhaseHeader =>
        Left(StreamingDecode.UnexpectedEndOfInput("reading a top-level token", m.carryLen.toLong))
      case PhaseSkipPayload =>
        Left(StreamingDecode.UnexpectedEndOfInput("skipping a stream payload", m.remaining))
      case PhaseStreamTrailer =>
        Left(StreamingDecode.UnexpectedEndOfInput("reading endstream/endobj", m.carryLen.toLong))
      case _ => Right(())

  def structuralCarryBytes(m: Machine): Long = m.carryLen.toLong

  /** Whole-chunk fast path — one window, no incremental carry merge. */
  def scanChunk(bytes: Chunk[Byte], maxCarryBytes: Int): Either[PdfObjectScanner.Error, Chunk[PdfObjectScanner.Boundary]] =
    bytes match
      case Chunk.ByteArray(arr, off, len) =>
        val m = Machine.fresh()
        run(m, arr, off, len, maxCarryBytes, inputBase = off.toLong, bytesSeenAfter = off.toLong + len.toLong).flatMap { found =>
          validate(m).left
            .map(e => PdfObjectScanner.Error.UnexpectedEnd(e.context, e.remainingBytes))
            .map(_ => found)
        }
      case other =>
        val arr = other.toArray
        scanChunk(Chunk.fromArray(arr), maxCarryBytes)

  def step(
    state: State,
    buf: Array[Byte],
    offset: Int,
    length: Int,
    maxCarryBytes: Int
  ): Either[PdfObjectScanner.Error, Chunk[PdfObjectScanner.Boundary]] =
    try
      val m = state.machine
      val oldCarry = m.carryLen
      val oldBytesSeen = m.bytesSeen
      val (work, workOff, workLen, inputBase) =
        if oldCarry == 0 then (buf, offset, length, oldBytesSeen)
        else
          val merged = m.mergeCarry(buf, offset, length)
          (merged._1, merged._2, merged._3, oldBytesSeen - oldCarry.toLong)

      run(m, work, workOff, workLen, maxCarryBytes, inputBase, bytesSeenAfter = oldBytesSeen + length.toLong)
    catch
      case StreamingDecode.CarryLimitExceeded(max, observed) =>
        Left(PdfObjectScanner.Error.CarryLimit(max, observed))
      case e: StreamingDecode.UnresolvedIndirectStreamLength =>
        Left(PdfObjectScanner.Error.IndirectLength(e.index, e.reference))
      case e: StreamingDecode.InvalidDeclaredStreamLength =>
        Left(PdfObjectScanner.Error.Malformed(e.getMessage, e))
      case scala.util.control.NonFatal(e) =>
        Left(PdfObjectScanner.Error.Malformed(e.getMessage, e))

  private def carryIsTriviaOnly(m: Machine): Boolean =
    var i = 0
    while i < m.carryLen do
      val b = m.carryBuf(i)
      if isWs(b) then i += 1
      else if b == '%'.toByte && !(i + 1 < m.carryLen && m.carryBuf(i + 1) == '%'.toByte) then
        i += 1
        while i < m.carryLen && m.carryBuf(i) != '\n'.toByte && m.carryBuf(i) != '\r'.toByte do i += 1
      else return false
    true

  private def retainCarry(m: Machine, work: Array[Byte], from: Int, end: Int, maxCarryBytes: Int): Unit =
    val hold = end - from
    if hold > maxCarryBytes then throw StreamingDecode.CarryLimitExceeded(maxCarryBytes, hold.toLong)
    m.retainWork(work, from, end)

  private def run(
    m: Machine,
    work: Array[Byte],
    workOff: Int,
    workLen: Int,
    maxCarryBytes: Int,
    inputBase: Long,
    bytesSeenAfter: Long
  ): Either[PdfObjectScanner.Error, Chunk[PdfObjectScanner.Boundary]] =
    try
      val out = Chunk.newBuilder[PdfObjectScanner.Boundary]
      var pos = workOff
      val end = workOff + workLen
      var retainStart = -1
      var suspend = false

      def suspendAt(from: Int): Unit =
        retainStart = from
        suspend = true

      def runPhase(): Boolean =
        m.phase match
          case PhaseHeader => false
          case PhaseSkipPayload =>
            if m.remaining > 0L then
              val avail = (end - pos).toLong
              if avail <= 0L then true
              else
                val take = math.min(m.remaining, avail)
                pos += take.toInt
                m.remaining -= take
                if m.remaining == 0L then m.phase = PhaseStreamTrailer
                false
            else
              m.phase = PhaseStreamTrailer
              false
          case PhaseStreamTrailer =>
            val trailerEnd = consumeStreamTrailer(work, end, pos)
            if trailerEnd < 0 then true
            else
              pos = trailerEnd
              if m.emitBoundary then out += PdfObjectScanner.Boundary(Obj.Index(m.objNum, m.objGen), inputBase + pos - workOff)
              m.phase = PhaseHeader
              m.clearInFlight()
              false
          case _ => false

      while !suspend && (pos < end || m.phase != PhaseHeader) do
        if m.phase != PhaseHeader then
          if runPhase() then suspend = true
        else if pos >= end then suspend = true
        else
          val headerStart = pos
          skipTrivia(work, end, pos) match
            case None => suspendAt(headerStart)
            case Some(p0) =>
              pos = p0
              if pos >= end then suspendAt(headerStart)
              else if kwAt(work, pos, end, pdfMagic) then
                parseVersion(work, end, pos) match
                  case None       => suspendAt(headerStart)
                  case Some(next) => pos = next
              else if kwAt(work, pos, end, xrefKw) then
                skipXrefSection(work, end, pos) match
                  case None =>
                    DuplicateFilterState.enterUpdateMode(m.dup)
                    suspendAt(headerStart)
                  case Some(next) =>
                    DuplicateFilterState.enterUpdateMode(m.dup)
                    pos = next
              else if isDigit(work(pos)) then
                parseObject(work, end, pos, m) match
                  case Left(err)        => throw err
                  case Right(None)      => suspendAt(headerStart)
                  case Right(Some(nextPos)) =>
                    pos = nextPos
                    if m.emitBoundary && m.phase == PhaseHeader then
                      out += PdfObjectScanner.Boundary(Obj.Index(m.objNum, m.objGen), inputBase + pos - workOff)
                      m.clearInFlight()
              else if work(pos) == '%'.toByte && (pos + 1 >= end || work(pos + 1) != '%'.toByte) then
                skipComment(work, end, pos) match
                  case None       => suspendAt(headerStart)
                  case Some(next) => pos = next
              else if kwAt(work, pos, end, startxrefKw) then
                DuplicateFilterState.enterUpdateMode(m.dup)
                pos = skipStartXrefTail(work, end, pos)
              else suspendAt(headerStart)

      if retainStart >= 0 && retainStart < end then
        retainCarry(m, work, retainStart, end, maxCarryBytes)
      else if suspend && m.phase != PhaseHeader && pos < end then
        retainCarry(m, work, pos, end, maxCarryBytes)
      else
        m.carryLen = 0

      m.bytesSeen = bytesSeenAfter

      Right(out.result())
    catch
      case StreamingDecode.CarryLimitExceeded(max, observed) =>
        Left(PdfObjectScanner.Error.CarryLimit(max, observed))
      case e: StreamingDecode.UnresolvedIndirectStreamLength =>
        Left(PdfObjectScanner.Error.IndirectLength(e.index, e.reference))
      case e: StreamingDecode.InvalidDeclaredStreamLength =>
        Left(PdfObjectScanner.Error.Malformed(e.getMessage, e))
      case scala.util.control.NonFatal(e) =>
        Left(PdfObjectScanner.Error.Malformed(e.getMessage, e))

  /** @return next position after object, or None if more bytes are needed */
  private def parseObject(
    arr: Array[Byte],
    end: Int,
    from: Int,
    m: Machine
  ): Either[Throwable, Option[Int]] =
    val (numberStart, numberEnd) = readDigits(arr, end, from)
    if numberEnd == numberStart then return Right(None)
    val generationStart = skipWs(arr, end, numberEnd)
    val (_, generationEnd) = readDigits(arr, end, generationStart)
    if generationEnd == generationStart then return Right(None)
    val objectMarker = skipWs(arr, end, generationEnd)
    if !kwAt(arr, objectMarker, end, objKw) then return Right(None)
    val body = skipWs(arr, end, objectMarker + objKw.length)
    if body >= end then return Right(None)

    for
      number     <- parseLong(arr, numberStart, numberEnd)
      generation <- parseLong(arr, generationStart, generationEnd)
      _          <-
        if generation <= Int.MaxValue.toLong then Right(())
        else Left(IllegalArgumentException("object generation overflows Int"))
    yield
      m.commitObjectHeader(number, generation.toInt)
      m.phase = PhaseHeader

      if body + 1 < end && arr(body) == '<'.toByte && arr(body + 1) == '<'.toByte then
        val dictEnd = scanDictionaryEnd(arr, end, body)
        if dictEnd < 0 then None
        else
          val marker = skipWs(arr, end, dictEnd)
          if streamPrefixIncomplete(arr, end, marker) then None
          else if kwAt(arr, marker, end, streamKw) then
            val nl = streamNewlineSize(arr, end, marker)
            if nl == 0 then None
            else
              rawLengthInDict(arr, body + 2, dictEnd - 2) match
                case Left(error) =>
                  throw StreamingDecode.InvalidDeclaredStreamLength(Obj.Index(number, generation.toInt), error.getMessage)
                case Right(RawLength.Indirect(ref)) =>
                  throw StreamingDecode.UnresolvedIndirectStreamLength(Obj.Index(number, generation.toInt), ref)
                case Right(RawLength.Direct(length)) =>
                  val payloadStart = marker + streamKw.length + nl
                  val payloadEnd   = payloadStart + length.toInt
                  if payloadEnd > end then
                    val inBuffer = (end - payloadStart).toLong
                    m.phase = PhaseSkipPayload
                    m.remaining = length - inBuffer
                    Some(end)
                  else
                    val trailerEnd = consumeStreamTrailer(arr, end, payloadEnd)
                    if trailerEnd < 0 then None else Some(trailerEnd)
          else
            val endobjAt = findEndobj(arr, end, marker)
            if endobjAt < 0 then None
            else
              val after = consumeEndobj(arr, end, endobjAt)
              if after < 0 then None else Some(after)
      else
        val endobjAt = findEndobj(arr, end, body)
        if endobjAt < 0 then None
        else
          val after = consumeEndobj(arr, end, endobjAt)
          if after < 0 then None else Some(after)

  private def streamPrefixIncomplete(arr: Array[Byte], end: Int, at: Int): Boolean =
    val rem = end - at
    rem > 0 && rem < streamKw.length && kwPrefix(arr, at, end, streamKw)

  private def kwPrefix(arr: Array[Byte], pos: Int, end: Int, kw: Array[Byte]): Boolean =
    val rem = end - pos
    rem > 0 && rem <= kw.length && {
      var i = 0
      while i < rem do
        if arr(pos + i) != kw(i) then return false
        i += 1
      true
    }

  private def isWs(b: Byte): Boolean =
    b == ' '.toByte || b == '\n'.toByte || b == '\r'.toByte || b == '\t'.toByte

  private def isDigit(b: Byte): Boolean =
    b >= '0'.toByte && b <= '9'.toByte

  private def isDelim(b: Byte): Boolean =
    isWs(b) ||
      b == '('.toByte || b == ')'.toByte ||
      b == '<'.toByte || b == '>'.toByte ||
      b == '['.toByte || b == ']'.toByte ||
      b == '{'.toByte || b == '}'.toByte ||
      b == '/'.toByte || b == '%'.toByte

  private def kwAt(arr: Array[Byte], pos: Int, end: Int, kw: Array[Byte]): Boolean =
    val klen = kw.length
    if pos < 0 || pos + klen > end then false
    else
      var i = 0
      while i < klen do
        if arr(pos + i) != kw(i) then return false
        i += 1
      true

  private def tokenAt(arr: Array[Byte], pos: Int, end: Int, kw: Array[Byte]): Boolean =
    kwAt(arr, pos, end, kw) &&
      (pos == 0 || isDelim(arr(pos - 1))) &&
      (pos + kw.length >= end || isDelim(arr(pos + kw.length)))

  private def skipWs(arr: Array[Byte], end: Int, from: Int): Int =
    var i = from
    while i < end && isWs(arr(i)) do i += 1
    i

  private def newlineSize(arr: Array[Byte], end: Int, at: Int): Int =
    if at >= end then 0
    else if arr(at) == '\r'.toByte && at + 1 < end && arr(at + 1) == '\n'.toByte then 2
    else if arr(at) == '\n'.toByte || arr(at) == '\r'.toByte then 1
    else 0

  private def skipPastNewline(arr: Array[Byte], end: Int, nlAt: Int): Int =
    nlAt + newlineSize(arr, end, nlAt)

  private def readDigits(arr: Array[Byte], end: Int, from: Int): (Int, Int) =
    var i = from
    while i < end && isDigit(arr(i)) do i += 1
    (from, i)

  private def parseLong(arr: Array[Byte], from: Int, until: Int): Either[Throwable, Long] =
    if until <= from then Left(IllegalArgumentException("empty number"))
    else
      var n        = 0L
      var i        = from
      var overflow = false
      while i < until && !overflow do
        if n > (Long.MaxValue - 9L) / 10L then overflow = true
        else
          n = n * 10L + (arr(i) - '0'.toByte).toLong
          i += 1
      if overflow then Left(IllegalArgumentException("number overflows Long"))
      else Right(n)

  private def skipTrivia(arr: Array[Byte], end: Int, from: Int): Option[Int] =
    var i = from
    var continue = true
    while continue && i < end do
      i = skipWs(arr, end, i)
      if i < end && arr(i) == '%'.toByte && !(i + 1 < end && arr(i + 1) == '%'.toByte) then
        skipComment(arr, end, i) match
          case None    => return None
          case Some(p) => i = p
      else continue = false
    Some(i)

  private def skipComment(arr: Array[Byte], end: Int, from: Int): Option[Int] =
    var i = from + 1
    while i < end && arr(i) != '\n'.toByte && arr(i) != '\r'.toByte do i += 1
    if i >= end then None
    else Some(skipPastNewline(arr, end, i))

  private def parseVersion(arr: Array[Byte], end: Int, from: Int): Option[Int] =
    if !kwAt(arr, from, end, pdfMagic) then Some(from)
    else
      val majStart = from + pdfMagic.length
      val (_, majEnd) = readDigits(arr, end, majStart)
      if majEnd == majStart || majEnd >= end || arr(majEnd) != '.'.toByte then None
      else
        val (_, minEnd) = readDigits(arr, end, majEnd + 1)
        if minEnd == majEnd + 1 then None
        else
          val p = skipWs(arr, end, minEnd)
          if p < end && arr(p) == '%'.toByte && (p + 1 >= end || arr(p + 1) != '%'.toByte) then
            skipComment(arr, end, p)
          else Some(p)

  private def skipXrefSection(arr: Array[Byte], end: Int, from: Int): Option[Int] =
    var i = from + xrefKw.length
    while i <= end - eofKw.length do
      if kwAt(arr, i, end, eofKw) then return Some(i + eofKw.length)
      i += 1
    None

  private def skipStartXrefTail(arr: Array[Byte], end: Int, from: Int): Int =
    var i = from
    while i < end do
      if kwAt(arr, i, end, eofKw) then return i + eofKw.length
      i += 1
    end

  private enum RawLength:
    case Direct(value: Long)
    case Indirect(reference: Prim.Ref)

  private def nameIs(arr: Array[Byte], from: Int, until: Int, name: Array[Byte]): Boolean =
    until - from == name.length && {
      var i = 0
      while i < name.length do
        if arr(from + i) != name(i) then return false
        i += 1
      true
    }

  private def rawLengthInDict(arr: Array[Byte], from: Int, until: Int): Either[Throwable, RawLength] =
    var index        = from
    var depth        = 1
    var literalDepth = 0
    var escaped      = false
    var hexString    = false
    var comment      = false

    while index < until do
      val byte = arr(index)
      if comment then
        if byte == '\n'.toByte || byte == '\r'.toByte then comment = false
        index += 1
      else if literalDepth > 0 then
        if escaped then escaped = false
        else if byte == '\\'.toByte then escaped = true
        else if byte == '('.toByte then literalDepth += 1
        else if byte == ')'.toByte then literalDepth -= 1
        index += 1
      else if hexString then
        if byte == '>'.toByte then hexString = false
        index += 1
      else if byte == '%'.toByte then
        comment = true
        index += 1
      else if byte == '('.toByte then
        literalDepth = 1
        index += 1
      else if byte == '<'.toByte && index + 1 < until && arr(index + 1) == '<'.toByte then
        depth += 1
        index += 2
      else if byte == '>'.toByte && index + 1 < until && arr(index + 1) == '>'.toByte then
        depth -= 1
        index += 2
      else if byte == '<'.toByte then
        hexString = true
        index += 1
      else if depth == 1 && byte == '/'.toByte then
        val nameStart = index + 1
        var nameEnd   = nameStart
        while nameEnd < until && !isDelim(arr(nameEnd)) do nameEnd += 1
        if nameIs(arr, nameStart, nameEnd, lengthName) then
          val firstStart = skipWs(arr, until, nameEnd)
          val (_, firstEnd) = readDigits(arr, until, firstStart)
          if firstEnd == firstStart then return Left(IllegalArgumentException("stream /Length is not numeric"))
          parseLong(arr, firstStart, firstEnd) match
            case Left(error) => return Left(error)
            case Right(first) =>
              val secondStart = skipWs(arr, until, firstEnd)
              val (_, secondEnd) = readDigits(arr, until, secondStart)
              if secondEnd > secondStart then
                val marker = skipWs(arr, until, secondEnd)
                if marker < until && arr(marker) == 'R'.toByte then
                  parseLong(arr, secondStart, secondEnd) match
                    case Left(error) => return Left(error)
                    case Right(generation) if generation <= Int.MaxValue.toLong =>
                      return Right(RawLength.Indirect(Prim.Ref(first, generation.toInt)))
                    case Right(_) => return Left(IllegalArgumentException("stream /Length generation overflows Int"))
              return Right(RawLength.Direct(first))
        index = nameEnd
      else index += 1

    Left(IllegalArgumentException("stream dictionary has no /Length"))

  private def scanDictionaryEnd(arr: Array[Byte], end: Int, start: Int): Int =
    if start + 2 > end || arr(start) != '<'.toByte || arr(start + 1) != '<'.toByte then -1
    else
      var index        = start + 2
      var depth        = 1
      var literalDepth = 0
      var escaped      = false
      var hexString    = false
      var comment      = false
      var dictEnd      = -1
      while index < end && dictEnd < 0 do
        val byte = arr(index)
        if comment then
          if byte == '\n'.toByte || byte == '\r'.toByte then comment = false
          index += 1
        else if literalDepth > 0 then
          if escaped then escaped = false
          else if byte == '\\'.toByte then escaped = true
          else if byte == '('.toByte then literalDepth += 1
          else if byte == ')'.toByte then literalDepth -= 1
          index += 1
        else if hexString then
          if byte == '>'.toByte then hexString = false
          index += 1
        else if byte == '%'.toByte then
          comment = true
          index += 1
        else if byte == '('.toByte then
          literalDepth = 1
          index += 1
        else if byte == '<'.toByte && index + 1 < end && arr(index + 1) == '<'.toByte then
          depth += 1
          index += 2
        else if byte == '>'.toByte && index + 1 < end && arr(index + 1) == '>'.toByte then
          depth -= 1
          index += 2
          if depth == 0 then dictEnd = index
        else if byte == '<'.toByte then
          hexString = true
          index += 1
        else index += 1
      dictEnd

  private def findEndobj(arr: Array[Byte], end: Int, from: Int): Int =
    var index        = from
    var dictDepth    = 0
    var arrayDepth   = 0
    var literalDepth = 0
    var escaped      = false
    var hexString    = false
    var comment      = false
    var found        = -1
    while index < end && found < 0 do
      val byte = arr(index)
      if comment then
        if byte == '\n'.toByte || byte == '\r'.toByte then comment = false
        index += 1
      else if literalDepth > 0 then
        if escaped then escaped = false
        else if byte == '\\'.toByte then escaped = true
        else if byte == '('.toByte then literalDepth += 1
        else if byte == ')'.toByte then literalDepth -= 1
        index += 1
      else if hexString then
        if byte == '>'.toByte then hexString = false
        index += 1
      else if byte == '%'.toByte then
        comment = true
        index += 1
      else if byte == '('.toByte then
        literalDepth = 1
        index += 1
      else if byte == '/'.toByte then
        index += 1
        while index < end && !isDelim(arr(index)) do index += 1
      else if byte == '<'.toByte && index + 1 < end && arr(index + 1) == '<'.toByte then
        dictDepth += 1
        index += 2
      else if byte == '>'.toByte && index + 1 < end && arr(index + 1) == '>'.toByte then
        if dictDepth > 0 then dictDepth -= 1
        index += 2
      else if byte == '<'.toByte then
        hexString = true
        index += 1
      else if byte == '['.toByte then
        arrayDepth += 1
        index += 1
      else if byte == ']'.toByte then
        if arrayDepth > 0 then arrayDepth -= 1
        index += 1
      else if dictDepth == 0 && arrayDepth == 0 && tokenAt(arr, index, end, endobjKw) then
        found = index
      else index += 1
    found

  private def streamNewlineSize(arr: Array[Byte], end: Int, streamAt: Int): Int =
    val after = streamAt + streamKw.length
    if after >= end then 0
    else if arr(after) == '\n'.toByte then 1
    else if arr(after) == '\r'.toByte && after + 1 >= end then 0
    else if arr(after) == '\r'.toByte && arr(after + 1) == '\n'.toByte then 2
    else 0

  private def consumeEndobj(arr: Array[Byte], end: Int, from: Int): Int =
    val afterKw = skipWs(arr, end, from)
    if !kwAt(arr, afterKw, end, endobjKw) then -1
    else skipWs(arr, end, afterKw + endobjKw.length)

  private def consumeStreamTrailer(arr: Array[Byte], end: Int, from: Int): Int =
    val afterStream = skipWs(arr, end, from)
    if !kwAt(arr, afterStream, end, endstreamKw) then -1
    else
      val afterEndstream = skipWs(arr, end, afterStream + endstreamKw.length)
      if !kwAt(arr, afterEndstream, end, endobjKw) then -1
      else skipWs(arr, end, afterEndstream + endobjKw.length)
}
