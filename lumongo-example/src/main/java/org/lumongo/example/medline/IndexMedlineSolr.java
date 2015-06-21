package org.lumongo.example.medline;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.lumongo.example.medline.schema.MedlineCitation;
import org.lumongo.util.LogUtil;
import org.lumongo.xml.StaxJAXBReader;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexMedlineSolr {

	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(IndexMedlineSolr.class);

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: directoryWithXml solrServerUrl");
			System.out.println(" ex. /tmp/medline http://127.0.0.1:8983/solr/medline-example");
			System.exit(1);
		}

		String medlineDirectory = args[0];
		String server = args[1];

		if (!(new File(medlineDirectory)).exists()) {
			System.out.println("Directory <" + medlineDirectory + "> does not exist");
			System.exit(2);
		}

		LogUtil.loadLogConfig();

		SolrClient solrClient = new HttpSolrClient(server);

		final AtomicInteger counter = new AtomicInteger();
		final long start = System.currentTimeMillis();

		StaxJAXBReader<MedlineCitation> s = new MedlineJAXBReader(MedlineCitation.class, "MedlineCitation") {

			@Override
			public void handleMedlineDocument(MedlineDocument medlineDocument) throws Exception {

				SolrInputDocument solrDoc = new SolrInputDocument();
				solrDoc.addField("id", medlineDocument.getPmid());
				solrDoc.addField("title", medlineDocument.getTitle());
				solrDoc.addField("journalTitle", medlineDocument.getJournalTitle());
				solrDoc.addField("abstractText", medlineDocument.getAbstractText());
				solrDoc.addField("publicationDate", medlineDocument.getPublicationDate());
				solrDoc.addField("journalVolume", medlineDocument.getJournalVolume());
				solrDoc.addField("journalIssue", medlineDocument.getJournalIssue());
				solrDoc.addField("journalCountry", medlineDocument.getJournalCountry());
				solrDoc.addField("journalCountryFacet", medlineDocument.getJournalCountry());
				solrDoc.addField("issn", medlineDocument.getIssn());
				solrDoc.addField("issnFacet", medlineDocument.getIssn());
				solrDoc.addField("authors", medlineDocument.getAuthors());
				solrDoc.addField("authorsExact", medlineDocument.getAuthors());

				solrClient.add(solrDoc);

				int c = counter.incrementAndGet();
				if (c % 50000 == 0) {
					long timeSinceStart = System.currentTimeMillis() - start;
					System.out.println(timeSinceStart + "\t" + c);
				}
			}

		};

		Path medlineXmlDirectory = Paths.get(medlineDirectory);

		try (DirectoryStream<Path> directory = Files.newDirectoryStream(medlineXmlDirectory)) {
			for (Path file : directory) {
				System.out.println("Found <" + file.toAbsolutePath().toString() + ">");
				if (file.toAbsolutePath().toString().endsWith("xml")) {
					try {
						s.handleFile(file.toAbsolutePath().toString());
					}
					catch (Exception e) {
						System.err.println("Failed to process <" + file.toAbsolutePath().toString() + ">: " + e);
					}
				}
			}
		}

	}

}
