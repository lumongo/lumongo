package org.lumongo.xml;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public abstract class StaxJAXBReader<T> {

    private Class<T> documentJAXBClass;
    private String documentElementName;


    private JAXBContext ctx;
    private Unmarshaller unmarshaller;
    private XMLInputFactory xmlInputFactory;

    public StaxJAXBReader(Class<T> documentJAXBClass, String documentElementName)
            throws JAXBException {

        this.documentJAXBClass = documentJAXBClass;
        this.documentElementName = documentElementName;


        this.ctx = JAXBContext.newInstance(documentJAXBClass);

        this.unmarshaller = ctx.createUnmarshaller();

        this.xmlInputFactory = XMLInputFactory.newInstance();

    }

    public void handleFile(String filename) throws Exception {

        try (Reader reader = new InputStreamReader(new FileInputStream(filename), Charset.forName("UTF-8"))) {

            XMLEventReader xmlEventReader = xmlInputFactory.createXMLEventReader(reader);

            while (xmlEventReader.hasNext()) {

                XMLEvent event = xmlEventReader.peek();
                if (event.isStartElement()) {
                    StartElement start = event.asStartElement();
                    if (documentElementName.equals(start.getName().getLocalPart())) {
                        JAXBElement<T> document = unmarshaller.unmarshal(xmlEventReader, documentJAXBClass);
                        handle(document.getValue());
                    }
                }
                xmlEventReader.next();

            }
        }


    }

    public abstract void handle(T item) throws Exception;
}
