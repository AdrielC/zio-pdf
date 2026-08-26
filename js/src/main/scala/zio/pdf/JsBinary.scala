package zio.pdf

import scala.annotation.nowarn
import scala.scalajs.js
import scala.scalajs.js.annotation.*
import scala.scalajs.js.typedarray.Uint8Array
import zio.Chunk

private[pdf] object JsBinary:

  def uint8(bytes: Array[Byte]): Uint8Array =
    val result = new Uint8Array(bytes.length)
    var index  = 0
    while index < bytes.length do
      result(index) = (bytes(index).toInt & 0xff).toShort
      index += 1
    result

  /** Copy only the requested bounded window into JavaScript-owned bytes. */
  def uint8(bytes: Chunk[Byte], from: Int, length: Int): Uint8Array =
    require(from >= 0 && length >= 0 && from <= bytes.size - length, "invalid byte window")
    val result = new Uint8Array(length)
    var index  = 0
    while index < length do
      result(index) = (bytes(from + index).toInt & 0xff).toShort
      index += 1
    result

  def bytes(uint8: Uint8Array): Array[Byte] =
    val result = new Array[Byte](uint8.length)
    var index  = 0
    while index < uint8.length do
      result(index) = uint8(index).toByte
      index += 1
    result

@js.native
@JSImport("pako", "deflate")
private[pdf] object PakoDeflate extends js.Object:
  def apply(input: Uint8Array): Uint8Array = js.native

@js.native
@JSImport("pako", "Inflate")
@nowarn("msg=unused explicit parameter")
private[pdf] class PakoInflate(options: js.Dictionary[js.Any]) extends js.Object:
  var onData: js.Function1[Uint8Array, Unit] = js.native
  val err: Int = js.native
  val msg: String = js.native
  def push(input: Uint8Array, finalChunk: Boolean): Boolean = js.native

@js.native
@JSImport("@noble/hashes/sha2.js", "sha256")
private[pdf] object NobleSha256 extends js.Object:
  def create(): NobleSha256State = js.native

@js.native
private[pdf] trait NobleSha256State extends js.Object:
  def update(input: Uint8Array): NobleSha256State = js.native
  def digest(): Uint8Array = js.native
