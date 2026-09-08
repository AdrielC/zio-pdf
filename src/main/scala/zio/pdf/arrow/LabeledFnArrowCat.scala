package zio.pdf.arrow

import ArrowObjects.*

/** [[Category]] for [[LabeledFnArrow]] — delegates algebra to underlying [[FnArrow]]. */
object LabeledFnArrowCat {

  given labeledCategory: Category[LabeledFnArrow] with {
    def id[A]: LabeledFnArrow[A, A] =
      LabeledFnArrow("id", FnArrow.id, 1, 1)

    def compose[A, B, C](f: LabeledFnArrow[B, C], g: LabeledFnArrow[A, B]): LabeledFnArrow[A, C] =
      LabeledFnArrow(s"${g.name}>>>${f.name}", g.arrow >>> f.arrow, g.inPorts, f.outPorts)

    def split[A: Ob, B: Ob, C: Ob, D: Ob](
        f: LabeledFnArrow[A, B],
        g: LabeledFnArrow[C, D]
    ): LabeledFnArrow[Prod[A, C], Prod[B, D]] =
      LabeledFnArrow(s"${f.name}***${g.name}", f.arrow *** g.arrow, f.inPorts + g.inPorts, f.outPorts + g.outPorts)

    def fanout[A: Ob, B: Ob, C: Ob](
        f: LabeledFnArrow[A, B],
        g: LabeledFnArrow[A, C]
    ): LabeledFnArrow[A, Prod[B, C]] =
      LabeledFnArrow(s"${f.name}&&&${g.name}", f.arrow &&& g.arrow, f.inPorts, f.outPorts + g.outPorts)

    def merge[A: Ob, B: Ob, C: Ob](
        f: LabeledFnArrow[A, C],
        g: LabeledFnArrow[B, C]
    ): LabeledFnArrow[Sum[A, B], C] =
      LabeledFnArrow(s"${f.name}|||${g.name}", FnArrowCat.fnCategory.merge(f.arrow, g.arrow), 2, 1)

    def choose[A: Ob, B: Ob, C: Ob, D: Ob](
        f: LabeledFnArrow[A, C],
        g: LabeledFnArrow[B, D]
    ): LabeledFnArrow[Sum[A, B], Sum[C, D]] =
      LabeledFnArrow(s"${f.name}+++${g.name}", FnArrowCat.fnCategory.choose(f.arrow, g.arrow), 2, 2)

    def injectLeft[A: Ob, B: Ob]: LabeledFnArrow[A, Sum[A, B]] =
      LabeledFnArrow("left", FnArrowCat.fnCategory.injectLeft, 1, 1)

    def injectRight[A: Ob, B: Ob]: LabeledFnArrow[B, Sum[A, B]] =
      LabeledFnArrow("right", FnArrowCat.fnCategory.injectRight, 1, 1)
  }
}
