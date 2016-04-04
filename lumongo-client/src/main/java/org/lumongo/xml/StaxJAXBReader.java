package org.lumongo.xml;

import org.lumongo.client.pool.WorkPool;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class StaxJAXBReader<T> {

	private final WorkPool pool;
	private Class<T> documentJAXBClass;
	private String documentElementName;
	private JAXBContext ctx;
	

	public StaxJAXBReader(Class<T> documentJAXBClass, String documentElementName, int threads) throws JAXBException {

		this.documentJAXBClass = documentJAXBClass;
		this.documentElementName = documentElementName;

		this.ctx = JAXBContext.newInstance(documentJAXBClass);



		this.pool = new WorkPool(threads);

	}

	public void handleFile(String filename) throws Exception {


		pool.executeAsync(() -> {



			try {
				Unmarshaller unmarshaller = ctx.createUnmarshaller();
				XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

				try (Reader reader = new InputStreamReader(new FileInputStream(filename), Charset.forName("UTF-8"))) {

					XMLEventReader xmlEventReader = xmlInputFactory.createXMLEventReader(reader);

					while (xmlEventReader.hasNext()) {

						XMLEvent event = xmlEventReader.peek();
						if (event.isStartElement()) {
							StartElement start = event.asStartElement();
							if (documentElementName.equals(start.getName().getLocalPart())) {
								JAXBElement<T> document = unmarshaller.unmarshal(xmlEventReader, documentJAXBClass);
								try {
									handle(document.getValue());
								}
								catch (Exception e) {
									handleException(e);
								}
							}
						}
						xmlEventReader.next();

					}
				}
			}
			catch (Exception e) {
				handleFileException(e);
			}
			return null;
		});

	}

	public void handleFileException(Exception e) {
		//Just throw the exception but allow subclasses to override this to continue
		throw new RuntimeException(e);
	}

	public void handleException(Exception e) {
		//Just throw the exception but allow subclasses to override this to continue
		throw new RuntimeException(e);
	}

	public abstract void handle(T item) throws Exception;

	public void shutdown() throws Exception {
		pool.shutdown();
	}
}
