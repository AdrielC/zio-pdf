package io.github.adrielc.scan

/** Optional pass-through observation syntax, expressed entirely by composition.
  * Prefer nominally distinct input/event types with the union form. Either
  * preserves provenance even when those types overlap. This is a Scala 3 build;
  * the Either representation does not imply a Scala 2 artifact is published.
  */
object ObservationSyntax:
  extension [I, O](observer: Scan[I, O])
    def withInputEither: Scan.Aux[(Unit, observer.State), I, Either[I, O]] =
      Scan.identity[I].broadcast(observer)
    def withInput: Scan.Aux[(Unit, observer.State), I, I | O] =
      observer.withInputEither.map[I | O] {
        case Left(input) => input
        case Right(observation) => observation
      }
