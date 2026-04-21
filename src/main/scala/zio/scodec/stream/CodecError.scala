/*
 * Copyright (c) 2013, Scodec
 * All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (see LICENSE for details).
 */

package zio.scodec.stream

import _root_.scodec.Err

/** Wraps a scodec [[Err]] in a [[Throwable]] so it can flow through ZIO error channels. */
final case class CodecError(err: Err) extends Exception(err.messageWithContext)
