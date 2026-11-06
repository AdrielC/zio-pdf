package io.github.adrielc.scan

/** A checked absolute range over an owner, independent of element/storage type.
  * Slicing changes only coordinates. The owner's lifetime and access rules still
  * apply; this descriptor neither copies data nor grants a longer lifetime.
  */
final class Span[+Owner] private (val owner: Owner, val from: Long, val until: Long):
  def length: Long = until - from
  def slice(from: Long, until: Long): Either[Span.InvalidRange, Span[Owner]] =
    if from < this.from || until < from || until > this.until then
      Left(Span.InvalidRange(from, until, this.from, this.until))
    else Right(new Span(owner, from, until))

object Span:
  final case class InvalidRange(from: Long, until: Long, availableFrom: Long, availableUntil: Long)
  def checked[Owner](owner: Owner, from: Long, until: Long): Either[InvalidRange, Span[Owner]] =
    if from < 0 || until < from then Left(InvalidRange(from, until, 0L, Long.MaxValue))
    else Right(new Span(owner, from, until))
