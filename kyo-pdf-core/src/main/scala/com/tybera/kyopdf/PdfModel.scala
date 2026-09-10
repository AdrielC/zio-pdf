package com.tybera.kyopdf

import kyo.{Abort, Schema, <}

/** Bounded values shared by the Kyo PDF parser, transforms, and adapters. */
final case class ByteLimit private (bytes: Long) derives Schema:
  def toLong: Long = bytes

object ByteLimit:
  def fromBytes(bytes: Long): ByteLimit < Abort[PdfError] =
    if bytes > 0 then ByteLimit(bytes)
    else Abort.fail(PdfError.InvalidPdf("PDF byte limit must be positive"))

  def mebibytes(value: Long): ByteLimit < Abort[PdfError] =
    if value > 0 && value <= Long.MaxValue / (1024L * 1024L) then ByteLimit(value * 1024L * 1024L)
    else Abort.fail(PdfError.InvalidPdf("Invalid PDF MiB limit"))

final case class RetentionLimits(
    maxObjects: Int = 50000,
    maxNodes: Int = 500000,
    maxDepth: Int = 64,
    maxPayloadBytes: Long = 64L * 1024L * 1024L
) derives Schema

final case class Facts(objects: Long, streams: Long, pages: Long) derives Schema

final case class Retention(objects: Int, nodes: Long, depth: Int, payloadBytes: Long) derives Schema

final case class ScanReport(
    version: String,
    facts: Facts,
    retention: Retention,
    encrypted: Boolean
) derives Schema

final case class ThumbnailInfo(
    objectNumber: Long,
    pageNumber: Long,
    width: Int,
    height: Int,
    compressedBytes: Int
) derives Schema

sealed trait PdfError derives Schema:
  def message: String

object PdfError:
  final case class TooLarge(limit: Long, observed: Long) extends PdfError:
    val message = s"PDF input is $observed bytes, above the configured $limit-byte limit"
  final case class InvalidPdf(reason: String) extends PdfError:
    val message = reason
  final case class RetentionExceeded(reason: String) extends PdfError:
    val message = reason
  final case class ThumbnailFailed(reason: String) extends PdfError:
    val message = reason
