package io.github.adrielc.bytescan.capture

import scala.language.experimental.captureChecking
import scala.annotation.experimental
import io.github.adrielc.bytescan.SharedScan.{AccessIssue, Block, View}

/** Opt-in capture-checked access to a borrowed byte range. The unchecked view
  * never escapes through this facade. Runtime expiration also protects callers
  * compiled without capture checking; backing storage remains runner-owned.
  */
@experimental
object ScopedView:
  sealed trait Borrowed extends caps.SharedCapability:
    def from: Long
    def until: Long
    def read(position: Long): Either[AccessIssue, Byte]
    def detach(from: Long, until: Long): Either[AccessIssue, Block]

  /** A may contain owned results, but cannot capture this callback's capability. */
  def use[A](view: View)(body: Borrowed^ => A): A =
    var active = true
    val borrowed = new Borrowed:
      def from: Long = view.start
      def until: Long = view.end
      def read(position: Long): Either[AccessIssue, Byte] =
        if active then view.read(position) else Left(AccessIssue.Expired)
      def detach(from: Long, until: Long): Either[AccessIssue, Block] =
        if active then view.detach(from, until) else Left(AccessIssue.Expired)
    // Pure memory borrow guard; no effectful resource acquisition occurs here.
    try body(borrowed)
    finally active = false
