package io.github.adrielc.bytescan.zio

import io.github.adrielc.bytescan.SharedScan.*
import _root_.zio.{Chunk, Scope, ZIO}
import _root_.zio.stream.ZStream

/** Scoped transport adapter. No ZIO type appears in the portable scan kernel. */
object ZioSharedScan:
  enum RunError[+InputError, +ScanError]:
    case Input(error: InputError)
    case Scan(error: Failure[ScanError])
    case Block(issues: Vector[BlockIssue])

  /** One ownership copy per bounded input chunk, shared by all scans. */
  def run[R, I, E, A](source: ZStream[R, I, Byte], scan: Probe[E, A], limits: Limits = Limits.default)
      : ZIO[R & Scope, RunError[I, E], A] =
    execute(source, scan, limits, chunk => Block.copyOf(chunk.length)(chunk.apply))

  /** Zero input copies. The caller supplies the retained backing-byte cost of
    * each immutable chunk, including any larger storage pinned by its views.
    * Input remains immutable until the enclosing scope has released all leases.
    */
  def runBorrowed[R, I, E, A](source: ZStream[R, I, Byte], scan: Probe[E, A],
      backingBytes: Chunk[Byte] => Long, limits: Limits = Limits.default): ZIO[R & Scope, RunError[I, E], A] =
    execute(source, scan, limits, chunk => Block.borrowImmutable(chunk.length, backingBytes(chunk))(chunk.apply))

  private def execute[R, I, E, A](source: ZStream[R, I, Byte], scan: Probe[E, A], limits: Limits,
      block: Chunk[Byte] => Either[Vector[BlockIssue], Block]): ZIO[R & Scope, RunError[I, E], A] =
    ZIO.acquireRelease(
      ZIO.fromEither(Session.open(scan, limits)).mapError(RunError.Scan.apply)
    )(session => ZIO.succeed(session.close())).flatMap { session =>
      val run = session.result match
        case Some(result) => ZIO.fromEither(result).mapError(RunError.Scan.apply)
        case None => source.toPull.flatMap { pull =>
          def loop: ZIO[R, RunError[I, E], A] = ZIO.suspendSucceed {
            pull.foldZIO(
              {
                case None => ZIO.fromEither(session.finish()).mapError(RunError.Scan.apply)
                case Some(error) => ZIO.fail(RunError.Input(error))
              },
              chunk =>
                if chunk.isEmpty then loop
                else ZIO.fromEither(block(chunk)).mapError(RunError.Block.apply).flatMap { bytes =>
                  ZIO.fromEither(session.offer(bytes)).mapError(RunError.Scan.apply).flatMap {
                    case Some(value) => ZIO.succeed(value)
                    case None => loop
                  }
                }
            )
          }
          loop
        }
      run.onError(_ => ZIO.succeed(session.close()))
    }
