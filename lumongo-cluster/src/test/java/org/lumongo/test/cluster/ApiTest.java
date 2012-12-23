package org.lumongo.test.cluster;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.lumongo.client.command.CreateIndex;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.command.UpdateIndex;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetNumberOfDocsResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.doc.IndexedDocBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ApiTest {
	private LumongoWorkPool lumongoWorkPool;

	public void startClient() throws Exception {
		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
		lumongoPoolConfig.addMember("localhost");
		lumongoPoolConfig.setDefaultRetries(0);
		lumongoPoolConfig.setMaxConnections(8);
		lumongoPoolConfig.setMaxIdle(8);
		lumongoPoolConfig.setCompressedConnection(false);
		lumongoPoolConfig.setPoolName(null);
		lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);

	}

	public void updateMembers() throws Exception {
		lumongoWorkPool.updateMembers();
	}

	public void stopClient() throws Exception {
		lumongoWorkPool.shutdown();
	}

	public void createIndex() throws Exception {
		String defaultSearchField = "title";
		int numberOfSegments = 16;
		String uniqueIdField = "uid";

		IndexConfig indexConfig = new IndexConfig(defaultSearchField);
		indexConfig.setDefaultAnalyzer(LMAnalyzer.KEYWORD);
		indexConfig.setFieldAnalyzer("title", LMAnalyzer.STANDARD);
		indexConfig.setFieldAnalyzer("issn", LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("an", LMAnalyzer.NUMERIC_INT);

		CreateIndex createIndex = new CreateIndex("myIndexName", numberOfSegments, uniqueIdField, indexConfig);
		createIndex.setFaceted(true);
		lumongoWorkPool.createIndex(createIndex);
	}

	public void updateIndex() throws Exception {

		String defaultSearchField = "abstract";
		IndexConfig indexConfig = new IndexConfig(defaultSearchField);
		indexConfig.setDefaultAnalyzer(LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("title", LMAnalyzer.STANDARD);
		indexConfig.setFieldAnalyzer("issn", LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("an", LMAnalyzer.NUMERIC_INT);
		indexConfig.setFieldAnalyzer("abstract", LMAnalyzer.STANDARD);

		UpdateIndex updateIndex = new UpdateIndex("myIndexName", indexConfig);
		lumongoWorkPool.updateIndex(updateIndex);
	}

	public void createOrUpdateIndex() throws Exception {
		String defaultSearchField = "abstract";
		int numberOfSegments = 16;
		String uniqueIdField = "uid";

		IndexConfig indexConfig = new IndexConfig(defaultSearchField);
		indexConfig.setDefaultAnalyzer(LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("title", LMAnalyzer.STANDARD);
		indexConfig.setFieldAnalyzer("issn", LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("an", LMAnalyzer.NUMERIC_INT);
		indexConfig.setFieldAnalyzer("abstract", LMAnalyzer.STANDARD);

		CreateOrUpdateIndex createOrUpdateIndex = new CreateOrUpdateIndex("myIndexName", numberOfSegments, uniqueIdField, indexConfig);
		CreateOrUpdateIndexResult result = lumongoWorkPool.createOrUpdateIndex(createOrUpdateIndex);
		System.out.println(result.isNewIndex());
		System.out.println(result.isUpdatedIndex());
	}

	public void storeDocumentText() throws Exception {

		IndexedDocBuilder docBuilder = new IndexedDocBuilder("myIndexName");
		docBuilder.addField("issn", "1234-1234");
		docBuilder.addField("title", "A really special title");
		docBuilder.addFacet("issn", "1234-1234");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		String xml = "<sampleXML></sampleXML>";

		Store s = new Store("myid111");
		s.addIndexedDocument(indexedDoc);
		s.setResultDocument(xml);
		// s.setResultDocument(xml, true); // store compressed

		lumongoWorkPool.store(s);
	}

	public void storeDocumentBson() throws Exception {

		IndexedDocBuilder docBuilder = new IndexedDocBuilder("myIndexName");
		docBuilder.addField("title", "Magic Java Beans");
		docBuilder.addField("issn", "4321-4321");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		DBObject dbObject = new BasicDBObject();
		dbObject.put("someKey", "someValue");
		dbObject.put("other key", "other value");

		Store s = new Store("myid222");
		s.addIndexedDocument(indexedDoc);
		s.setResultDocument(dbObject);

		lumongoWorkPool.store(s);

	}

	public void storeDocumentBinary() throws Exception {
		IndexedDocBuilder docBuilder = new IndexedDocBuilder("myIndexName");
		docBuilder.addField("title", "Another great book");
		docBuilder.addField("issn", "1111-1111");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		byte[] binary = new byte[] { 1, 2, 3 };
		Store s = new Store("myid333");
		s.addIndexedDocument(indexedDoc);
		s.setResultDocument(binary);

		lumongoWorkPool.store(s);
	}

	public void storeDocumentAndMeta() throws Exception {

		IndexedDocBuilder docBuilder = new IndexedDocBuilder("myIndexName");
		docBuilder.addField("issn", "1234-1234");
		docBuilder.addField("title", "Special title");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		String xml = "<sampleXML></sampleXML>";

		HashMap<String, String> metadata = new HashMap<String, String>();
		metadata.put("test1", "val1");
		metadata.put("test2", "val2");

		Store s = new Store("myid123");
		s.addIndexedDocument(indexedDoc);
		s.setResultDocument(xml, metadata);


		lumongoWorkPool.store(s);
	}

	public void fetchDocumentText() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid111");

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			String text = fetchResult.getDocumentAsUtf8();
			System.out.println(text);
		}
	}

	public void fetchDocumentBson() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid222");

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			DBObject object = fetchResult.getDocumentAsBson();
			System.out.println(object);
		}
	}

	public void fetchDocumentBinary() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid333");

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			byte[] bytes = fetchResult.getDocumentAsBytes();
			System.out.println(Arrays.toString(bytes));
		}
	}

	public void fetchDocumentMeta() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid123");

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			String text = fetchResult.getDocumentAsUtf8();
			System.out.println(text);

			Map<String, String> meta = fetchResult.getMeta();
			System.out.println(meta);
		}

	}

	public void simpleQuery() throws Exception {
		int numberOfResults = 10;
		String normalLuceneQuery = "issn:1234-1234 AND title:special";
		Query query = new Query("myIndexName", normalLuceneQuery, numberOfResults);

		QueryResult queryResult = lumongoWorkPool.query(query);

		long totalHits = queryResult.getTotalHits();

		for (ScoredResult sr : queryResult.getResults()) {
			System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + ">");
		}

		System.out.println("Query <" + normalLuceneQuery + "> found <" + totalHits + "> total hits.  Fetched <" + queryResult.getResults().size()
				+ "> documents.");
	}

	public void pagingQuery() throws Exception {
		int numberOfResults = 10;
		String normalLuceneQuery = "issn:1234-1234 AND title:special";
		Query query = new Query("myIndexName", normalLuceneQuery, numberOfResults);

		QueryResult firstResult = lumongoWorkPool.query(query);

		query.setLastResult(firstResult);

		QueryResult secondResult = lumongoWorkPool.query(query);

	}

	public void facetQuery() throws Exception {
		// Can set number of documents to return to 0 unless you want the documents
		// at the same time
		Query query = new Query("myIndexName", "title:userguide", 0);
		int maxFacets = 30;
		query.addCountRequest("issn", maxFacets);

		QueryResult queryResult = lumongoWorkPool.query(query);
		for (FacetCount fc : queryResult.getFacetCounts()) {
			System.out.println("Facet <" + fc.getFacet() + "> with count <" + fc.getCount() + ">");
		}
	}

	public void getCount() throws Exception {
		GetNumberOfDocsResult result = lumongoWorkPool.getNumberOfDocs("myIndexName");
		System.out.println(result.getNumberOfDocs());
	}

	public static void main(String[] args) throws Exception {
		ApiTest apiTest = new ApiTest();
		apiTest.startClient();
		try {
			apiTest.updateMembers();
			// apiTest.createIndex();
			// apiTest.updateIndex();
			apiTest.createOrUpdateIndex();
			apiTest.storeDocumentText();
			apiTest.storeDocumentBson();
			apiTest.storeDocumentBinary();
			apiTest.storeDocumentAndMeta();

			apiTest.fetchDocumentText();
			apiTest.fetchDocumentBson();
			apiTest.fetchDocumentBinary();
			apiTest.fetchDocumentMeta();

			apiTest.getCount();
		}
		catch (Exception e) {
			System.err.println(e);
		}
		finally {
			apiTest.stopClient();
		}
	}
}
