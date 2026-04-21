package zio.graviton.scan

object Fusion {

  def tryFuse[I, O](scan: FreeScan[I, O]): Option[I => O] =
    scan match {
      case FreeScan.Arr(f) =>
        Some(f.asInstanceOf[I => O])
      case FreeScan.Prim(ScanPrim.Map(f)) =>
        Some(f.asInstanceOf[I => O])
      case FreeScan.AndThen(left, right) =>
        for
          fl <- tryFuse(left.asInstanceOf[FreeScan[I, Any]])
          fr <- tryFuse(right.asInstanceOf[FreeScan[Any, O]])
        yield fl.andThen(fr)
      case _ =>
        None
    }
}
