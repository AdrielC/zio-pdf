package io.github.adrielc.kyopdf

import java.nio.charset.StandardCharsets.UTF_8
import scala.scalanative.unsafe.*
import scala.scalanative.unsigned.*
import scala.util.control.NonFatal

private[kyopdf] object XmpReader:
  private final class Session(val parser: Ptr[Byte], val events: XmpEvents):
    var failure: Throwable = null

  // Expat calls back synchronously on the calling thread. Never unwind Scala
  // exceptions through its C stack; stop parsing and rethrow after XML_Parse.
  private val current = new ThreadLocal[Session]
  private val Separator = '\u001f'

  private def name(ptr: CString): (String, String) =
    val full = fromCString(ptr)
    val split = full.indexOf(Separator)
    if split < 0 then ("", full) else (full.substring(0, split), full.substring(split + 1))

  private def attributes(ptr: Ptr[CString]): Iterator[(String, String, String)] =
    new Iterator[(String, String, String)]:
      private var index = 0
      def hasNext: Boolean = ptr != null && !(ptr + index) != null
      def next(): (String, String, String) =
        if !hasNext then throw new NoSuchElementException("XML attribute")
        val (namespace, local) = name(!(ptr + index))
        val value = fromCString(!(ptr + index + 1))
        index += 2
        (namespace, local, value)

  private def event(f: XmpEvents => Unit): Unit =
    val session = current.get()
    if session.failure == null then
      try
        session.events.event()
        f(session.events)
      catch
        case NonFatal(error) =>
          session.failure = error
          val _ = Expat.XML_StopParser(session.parser, 0.toUByte)

  private val start: CFuncPtr3[Ptr[Byte], CString, Ptr[CString], Unit] =
    CFuncPtr3.fromScalaFunction((_, element, attrs) => event { events =>
      val (namespace, local) = name(element)
      events.start(namespace, local, attributes(attrs))
    })
  private val end: CFuncPtr2[Ptr[Byte], CString, Unit] =
    CFuncPtr2.fromScalaFunction((_, _) => event(_.end()))
  private val characters: CFuncPtr3[Ptr[Byte], CString, CInt, Unit] =
    CFuncPtr3.fromScalaFunction((_, value, length) => event(_.characters {
      require(length <= 256, "PDF/A identification value exceeds its bound")
      val data = Array.tabulate[Byte](length)(i => !(value + i))
      new String(data, UTF_8)
    }))
  private val other: CFuncPtr3[Ptr[Byte], CString, CInt, Unit] =
    CFuncPtr3.fromScalaFunction((_, _, _) => event(_ => ()))
  private val doctype: CFuncPtr5[Ptr[Byte], CString, CString, CString, CInt, Unit] =
    CFuncPtr5.fromScalaFunction((_, _, _, _, _) =>
      event(_ => throw new IllegalArgumentException("DTD and entity declarations are disabled")))
  private val external: CFuncPtr5[Ptr[Byte], CString, CString, CString, CString, CInt] =
    CFuncPtr5.fromScalaFunction((_, _, _, _, _) => 0)

  def read(bytes: Array[Byte], events: XmpEvents): Unit =
    require(bytes.length <= 512 * 1024, "XMP byte limit exceeded")
    val parser = Expat.XML_ParserCreateNS(null, Separator.toByte)
    if parser == null then throw new IllegalStateException("Could not allocate XML parser")
    val previous = current.get()
    val session = new Session(parser, events)
    current.set(session)
    try
      Expat.XML_SetElementHandler(parser, start, end)
      Expat.XML_SetCharacterDataHandler(parser, characters)
      Expat.XML_SetDefaultHandler(parser, other)
      Expat.XML_SetStartDoctypeDeclHandler(parser, doctype)
      Expat.XML_SetExternalEntityRefHandler(parser, external)
      Zone {
        val input = alloc[Byte](bytes.length.max(1))
        var i = 0
        while i < bytes.length do
          !(input + i) = bytes(i)
          i += 1
        val status = Expat.XML_Parse(parser, input, bytes.length, 1)
        if session.failure != null then throw session.failure
        require(status == 1, "Malformed XMP metadata")
      }
    finally
      if previous == null then current.remove() else current.set(previous)
      Expat.XML_ParserFree(parser)

@link("expat")
@extern
private object Expat:
  def XML_ParserCreateNS(encoding: CString, separator: CChar): Ptr[Byte] = extern
  def XML_ParserFree(parser: Ptr[Byte]): Unit = extern
  def XML_Parse(parser: Ptr[Byte], bytes: CString, length: CInt, isFinal: CInt): CInt = extern
  def XML_StopParser(parser: Ptr[Byte], resumable: CUnsignedChar): CInt = extern
  def XML_SetElementHandler(parser: Ptr[Byte], start: CFuncPtr3[Ptr[Byte], CString, Ptr[CString], Unit],
      end: CFuncPtr2[Ptr[Byte], CString, Unit]): Unit = extern
  def XML_SetCharacterDataHandler(parser: Ptr[Byte], handler: CFuncPtr3[Ptr[Byte], CString, CInt, Unit]): Unit = extern
  def XML_SetDefaultHandler(parser: Ptr[Byte], handler: CFuncPtr3[Ptr[Byte], CString, CInt, Unit]): Unit = extern
  def XML_SetStartDoctypeDeclHandler(parser: Ptr[Byte],
      handler: CFuncPtr5[Ptr[Byte], CString, CString, CString, CInt, Unit]): Unit = extern
  def XML_SetExternalEntityRefHandler(parser: Ptr[Byte],
      handler: CFuncPtr5[Ptr[Byte], CString, CString, CString, CString, CInt]): Unit = extern
