package zio.graviton.scan

sealed trait SpineHead[-I, +M]

object SpineHead:
  final case class FromPrim[I, M](p: FreeScan.Prim[I, M]) extends SpineHead[I, M]
  final case class FromArr[I, M](a: FreeScan.Arr[I, M])   extends SpineHead[I, M]
  final case class FromFan[I, OL, OR](f: FreeScan.Fanout[I, OL, OR]) extends SpineHead[I, (OL, OR)]
  final case class FromChoice[IL, IR, O](c: FreeScan.Choice[IL, IR, O]) extends SpineHead[Either[IL, IR], O]

object Flatten:

  /** Left-associate `AndThen` into a head plus right spine (Volga-style reassociation). */
  def flattenSpine[I, O](scan: FreeScan[I, O]): (SpineHead[I, ?], List[FreeScan[?, O]]) =
    scan match
      case p: FreeScan.Prim[I, ?] => (SpineHead.FromPrim(p), Nil)
      case a: FreeScan.Arr[I, ?]  => (SpineHead.FromArr(a), Nil)
      case f: FreeScan.Fanout[I, ol, or] => (SpineHead.FromFan(f), Nil)
      case c: FreeScan.Choice[il, ir, o] => (SpineHead.FromChoice(c), Nil)
      case FreeScan.AndThen(l, r) =>
        val (prim, spine) = flattenSpine(l)
        (prim, (spine :+ r).asInstanceOf[List[FreeScan[?, O]]])
