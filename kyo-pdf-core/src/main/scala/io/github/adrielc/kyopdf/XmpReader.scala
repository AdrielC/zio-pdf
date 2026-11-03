package io.github.adrielc.kyopdf

import java.io.ByteArrayInputStream
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamException}
import scala.util.Using

private[kyopdf] object XmpReader:
  def read(bytes: Array[Byte], events: XmpEvents): Unit =
    val factory = XMLInputFactory.newDefaultFactory()
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, java.lang.Boolean.TRUE)
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, java.lang.Boolean.FALSE)
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, java.lang.Boolean.FALSE)
    factory.setXMLResolver((_, _, _, _) => throw XMLStreamException("External XML resources are disabled"))
    Using.resource(new ByteArrayInputStream(bytes)) { input =>
      val reader = factory.createXMLStreamReader(input)
      Using.resource(new AutoCloseable { def close(): Unit = reader.close() }) { _ =>
        while reader.hasNext do
          events.event()
          reader.next() match
            case XMLStreamConstants.DTD | XMLStreamConstants.ENTITY_REFERENCE =>
              throw XMLStreamException("DTD and entity declarations are disabled")
            case XMLStreamConstants.START_ELEMENT =>
              events.start(reader.getNamespaceURI, reader.getLocalName,
                (0 until reader.getAttributeCount).iterator.map { i =>
                  (reader.getAttributeNamespace(i), reader.getAttributeLocalName(i), reader.getAttributeValue(i))
                })
            case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA => events.characters(reader.getText)
            case XMLStreamConstants.END_ELEMENT => events.end()
            case _ => ()
      }
    }
