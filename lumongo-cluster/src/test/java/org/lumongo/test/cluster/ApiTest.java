package org.lumongo.test.cluster;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.lumongo.client.command.FetchAllAssociated;
import org.lumongo.client.command.FetchAssociated;
import org.lumongo.client.command.CreateIndex;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.FetchLargeAssociated;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.command.StoreLargeAssociated;
import org.lumongo.client.command.UpdateIndex;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetNumberOfDocsResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.client.result.StoreLargeAssociatedResult;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.doc.AssociatedBuilder;
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
	
	public void deleteIndex() throws Exception {		
		lumongoWorkPool.deleteIndex("myIndexName");
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

		HashMap<String, String> metadata = new HashMap<String, String>();
		metadata.put("test1", "val1");
		metadata.put("test2", "val2");

		Store s1 = new Store("myid1111");
		s1.addIndexedDocument(indexedDoc);
		s1.setResultDocument(xml, metadata);

		lumongoWorkPool.store(s1);
	}

	public void storeDocumentBson() throws Exception {

		IndexedDocBuilder docBuilder = new IndexedDocBuilder("myIndexName");
		docBuilder.addField("title", "Magic Java Beans");
		docBuilder.addField("issn", "4321-4321");
		docBuilder.addFacet("issn", "4321-4321");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		DBObject dbObject = new BasicDBObject();
		dbObject.put("someKey", "someValue");
		dbObject.put("other key", "other value");

		Store s = new Store("myid222");
		s.addIndexedDocument(indexedDoc);
		s.setResultDocument(dbObject, true);

		lumongoWorkPool.store(s);

		HashMap<String, String> metadata = new HashMap<String, String>();
		metadata.put("test1", "val1");
		metadata.put("test2", "val2");

		Store s1 = new Store("myid2222");
		s1.addIndexedDocument(indexedDoc);
		s1.setResultDocument(dbObject, metadata);

		lumongoWorkPool.store(s1);

	}

	public void storeDocumentBinary() throws Exception {
		IndexedDocBuilder docBuilder = new IndexedDocBuilder("myIndexName");
		docBuilder.addField("title", "Another great and special book");
		docBuilder.addField("issn", "1111-1111");
		docBuilder.addFacet("issn", "1111-1111");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		byte[] binary = new byte[] { 1, 2, 3 };
		Store s = new Store("myid333");
		s.addIndexedDocument(indexedDoc);
		s.setResultDocument(binary);

		lumongoWorkPool.store(s);

		HashMap<String, String> metadata = new HashMap<String, String>();
		metadata.put("test1", "val1");
		metadata.put("test2", "val2");

		Store s1 = new Store("myid3333");
		s1.addIndexedDocument(indexedDoc);
		s1.setResultDocument(binary, metadata);

		lumongoWorkPool.store(s1);
	}

	public void fetchDocumentText() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid111");

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			String text = fetchResult.getDocumentAsUtf8();
			System.out.println(text);
		}

		FetchDocument fetchDocument1 = new FetchDocument("myid1111");

		FetchResult fetchResult1 = lumongoWorkPool.fetch(fetchDocument1);

		if (fetchResult1.hasResultDocument()) {
			String text = fetchResult1.getDocumentAsUtf8();
			System.out.println(text);

			Map<String, String> meta = fetchResult1.getMeta();
			System.out.println(meta);
		}
	}

	public void fetchDocumentBson() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid222");

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			DBObject object = fetchResult.getDocumentAsBson();
			System.out.println(object);
		}

		FetchDocument fetchDocument1 = new FetchDocument("myid2222");

		FetchResult fetchResult1 = lumongoWorkPool.fetch(fetchDocument1);

		if (fetchResult1.hasResultDocument()) {
			DBObject object = fetchResult1.getDocumentAsBson();
			System.out.println(object);

			Map<String, String> meta = fetchResult1.getMeta();
			System.out.println(meta);
		}
	}

	public void fetchDocumentBinary() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid333");

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			byte[] bytes = fetchResult.getDocumentAsBytes();
			System.out.println(Arrays.toString(bytes));
		}

		FetchDocument fetchDocument1 = new FetchDocument("myid3333");

		FetchResult fetchResult1 = lumongoWorkPool.fetch(fetchDocument1);

		if (fetchResult1.hasResultDocument()) {
			byte[] bytes = fetchResult1.getDocumentAsBytes();
			System.out.println(Arrays.toString(bytes));

			Map<String, String> meta = fetchResult1.getMeta();
			System.out.println(meta);
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

		@SuppressWarnings("unused")
		long totalHits = queryResult.getTotalHits();

		for (ScoredResult sr : queryResult.getResults()) {
			System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + ">");
		}

	}

	public void pagingQuery() throws Exception {
		int numberOfResults = 2;
		String normalLuceneQuery = "issn:1234-1234 AND title:special";
		Query query = new Query("myIndexName", normalLuceneQuery, numberOfResults);

		QueryResult firstResult = lumongoWorkPool.query(query);

		query.setLastResult(firstResult);

		QueryResult secondResult = lumongoWorkPool.query(query);
		
		for (ScoredResult sr : secondResult.getResults()) {
			System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + ">");
		}

	}

	public void facetQuery() throws Exception {
		// Can set number of documents to return to 0 unless you want the documents
		// at the same time
		Query query = new Query("myIndexName", "title:special", 0);
		int maxFacets = 30;
		query.addCountRequest("issn", maxFacets);

		QueryResult queryResult = lumongoWorkPool.query(query);
		for (FacetCount fc : queryResult.getFacetCounts()) {
			System.out.println("Facet <" + fc.getFacet() + "> with count <" + fc.getCount() + ">");
		}
		
	}
	
	public void drillDownQuery() throws Exception {
		Query query = new Query("myIndexName", "title:special", 0);
		query.addDrillDown("issn", "1111-1111");
		QueryResult queryResult = lumongoWorkPool.query(query);
		for (FacetCount fc : queryResult.getFacetCounts()) {
			System.out.println("Facet <" + fc.getFacet() + "> with count <" + fc.getCount() + ">");
		}
	}

	
	
	public void getCount() throws Exception {
		GetNumberOfDocsResult result = lumongoWorkPool.getNumberOfDocs("myIndexName");
		System.out.println(result.getNumberOfDocs());
	}
	
	public void storeAssociated() throws Exception {
		String uniqueId = "myid123";
		String filename = "myfile2";
		
		AssociatedBuilder associatedBuilder = new AssociatedBuilder(uniqueId, filename);
		associatedBuilder.setCompressed(false);
		associatedBuilder.setDocument("Some Text3");
		associatedBuilder.addMetaData("mydata", "myvalue2");
		associatedBuilder.addMetaData("sometypeinfo", "text file2");
		AssociatedDocument ad = associatedBuilder.getAssociatedDocument();

		Store s = new Store(uniqueId);		
		s.addAssociatedDocument(ad);

		lumongoWorkPool.store(s);
	}
	
	public void fetchAssociated() throws Exception {
		String uniqueId = "myid123";
		
		FetchAllAssociated fetchAssociated = new FetchAllAssociated(uniqueId);
		

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchAssociated);
		System.out.println(fetchResult.getAssociatedDocuments());
	}

	public void storeLargeAssociated() throws Exception {
		String uniqueId = "myid333";
		String filename = "myfilename";
		
		StoreLargeAssociated storeLargeAssociated = new StoreLargeAssociated(uniqueId, filename, new File("/home/mdavis/Downloads/guice-3.0.zip"));
		
		lumongoWorkPool.storeLargeAssociated(storeLargeAssociated);
		
	}
	
	public void fetchLargeAssociated() throws Exception {
		String uniqueId = "myid333";
		String filename = "myfilename";
		
		FetchLargeAssociated fetchLargeAssociated = new FetchLargeAssociated(uniqueId, filename, new File("/home/mdavis/t.zip"));
		lumongoWorkPool.fetchLargeAssociated(fetchLargeAssociated);
		
	}
	
	public static void main(String[] args) throws Exception {
		ApiTest apiTest = new ApiTest();
		apiTest.startClient();
		try {
			apiTest.updateMembers();
			//apiTest.deleteIndex();
			// apiTest.createIndex();
			// apiTest.updateIndex();
			apiTest.createOrUpdateIndex();
			apiTest.storeDocumentText();
			apiTest.storeDocumentBson();
			apiTest.storeDocumentBinary();


			apiTest.fetchDocumentText();
			apiTest.fetchDocumentBson();
			apiTest.fetchDocumentBinary();
			
			apiTest.simpleQuery();
			apiTest.pagingQuery();
			apiTest.facetQuery();
			apiTest.drillDownQuery();

			apiTest.getCount();
			
			apiTest.storeAssociated();
			apiTest.fetchAssociated();
			
			apiTest.storeLargeAssociated();
			apiTest.fetchLargeAssociated();
		}
		catch (Exception e) {
			System.err.println(e);
		}
		finally {
			apiTest.stopClient();
		}
	}


}
