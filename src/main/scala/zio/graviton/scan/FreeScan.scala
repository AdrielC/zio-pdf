package zio.graviton.scan

/**
 * Free symmetric monoidal category description of a scan pipeline.
 * Replace with `FreeA` from AdrielC/free-arrow when that dependency lands.
 *
 * Full `Schema` derivation awaits `SchemaExpr` for [[ScanPrim.Map]] / [[ScanPrim.Filter]]
 * function fields (see design doc).
 */
sealed trait FreeScan[-I, +O]

object FreeScan {

  case class Prim[I, O](op: ScanPrim[I, StepOut[O]]) extends FreeScan[I, O]

  case class Arr[I, O](f: I => O) extends FreeScan[I, O]

  case class AndThen[I, M, O](left: FreeScan[I, M], right: FreeScan[M, O]) extends FreeScan[I, O]

  case class Fanout[I, OL, OR](left: FreeScan[I, OL], right: FreeScan[I, OR]) extends FreeScan[I, (OL, OR)]

  case class Choice[IL, IR, O](left: FreeScan[IL, O], right: FreeScan[IR, O]) extends FreeScan[Either[IL, IR], O]

  def lift[I, O](p: ScanPrim[I, StepOut[O]]): FreeScan[I, O] = Prim(p)

  def arr[I, O](f: I => O): FreeScan[I, O] = Arr(f)

  def id[A]: FreeScan[A, A] = Arr(identity)

  extension [I, O](left: FreeScan[I, O]) {
    def >>>[P](right: FreeScan[O, P]): FreeScan[I, P] = AndThen(left, right)

    def &&&[P](right: FreeScan[I, P]): FreeScan[I, (O, P)] = Fanout(left, right)

    def |||[I2](right: FreeScan[I2, O]): FreeScan[Either[I, I2], O] = Choice(left, right)

    def map[P](f: O => P): FreeScan[I, P] = AndThen(left, Arr(f))

    def contramap[J](f: J => I): FreeScan[J, O] = AndThen(Arr(f), left)

    def dimap[J, P](pre: J => I, post: O => P): FreeScan[J, P] =
      AndThen(Arr(pre), AndThen(left, Arr(post)))
  }
}
