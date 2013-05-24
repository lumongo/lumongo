package org.lumongo.test.cluster;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.lumongo.client.command.BatchFetch;
import org.lumongo.client.command.CreateIndex;
import org.lumongo.client.command.CreateOrUpdateIndex;
import org.lumongo.client.command.DeleteAllAssociated;
import org.lumongo.client.command.DeleteAssociated;
import org.lumongo.client.command.DeleteFromIndex;
import org.lumongo.client.command.DeleteFull;
import org.lumongo.client.command.FetchAllAssociated;
import org.lumongo.client.command.FetchAssociated;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.FetchLargeAssociated;
import org.lumongo.client.command.GetAllTerms;
import org.lumongo.client.command.GetFields;
import org.lumongo.client.command.GetNumberOfDocs;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.command.StoreLargeAssociated;
import org.lumongo.client.command.UpdateIndex;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.AssociatedResult;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetFieldsResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.GetMembersResult;
import org.lumongo.client.result.GetNumberOfDocsResult;
import org.lumongo.client.result.GetTermsResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMMember;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.Term;
import org.lumongo.doc.AssociatedBuilder;
import org.lumongo.doc.IndexedDocBuilder;
import org.lumongo.doc.ResultDocBuilder;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ApiTest {
	private static final String MY_INDEX_NAME = "myIndexName";
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

	public void deleteIndex(String indexName) throws Exception {
		lumongoWorkPool.deleteIndex(indexName);
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

		CreateIndex createIndex = new CreateIndex(MY_INDEX_NAME, numberOfSegments, uniqueIdField, indexConfig);
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

		UpdateIndex updateIndex = new UpdateIndex(MY_INDEX_NAME, indexConfig);
		lumongoWorkPool.updateIndex(updateIndex);
	}

	public void createOrUpdateIndex(String indexName) throws Exception {
		String defaultSearchField = "abstract";
		int numberOfSegments = 16;
		String uniqueIdField = "uid";

		IndexConfig indexConfig = new IndexConfig(defaultSearchField);
		indexConfig.setDefaultAnalyzer(LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("title", LMAnalyzer.STANDARD);
		indexConfig.setFieldAnalyzer("issn", LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("an", LMAnalyzer.NUMERIC_INT);
		indexConfig.setFieldAnalyzer("abstract", LMAnalyzer.STANDARD);

		CreateOrUpdateIndex createOrUpdateIndex = new CreateOrUpdateIndex(indexName, numberOfSegments, uniqueIdField, indexConfig);
		createOrUpdateIndex.setFaceted(true);
		CreateOrUpdateIndexResult result = lumongoWorkPool.createOrUpdateIndex(createOrUpdateIndex);
		System.out.println(result.isNewIndex());
		System.out.println(result.isUpdatedIndex());
	}

	public void storeDocumentText(String indexName, String uniqueId) throws Exception {

		IndexedDocBuilder docBuilder = new IndexedDocBuilder();
		docBuilder.addField("issn", "1234-1234");
		docBuilder.addField("title", "A really special title");
		docBuilder.addFacet("issn", "1234-1234");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		String xml = "<sampleXML></sampleXML>";

		Store s = new Store(uniqueId, indexName);
		s.setIndexedDocument(indexedDoc);
		ResultDocBuilder resultDocumentBuilder = new ResultDocBuilder();
		resultDocumentBuilder.setDocument(xml);
		s.setResultDocument(resultDocumentBuilder);

		// s.setResultDocument(xml, true); // store compressed

		lumongoWorkPool.store(s);

		Store s1 = new Store(uniqueId + "-meta", indexName);
		s1.setIndexedDocument(indexedDoc);

		ResultDocBuilder resultDocumentBuilder1 = new ResultDocBuilder();
		resultDocumentBuilder1.setDocument(xml);
		resultDocumentBuilder1.addMetaData("test1", "val1");
		resultDocumentBuilder1.addMetaData("test2", "val2");
		resultDocumentBuilder1.setCompressed(true);
		s1.setResultDocument(resultDocumentBuilder1);

		lumongoWorkPool.store(s1);
	}

	public void storeDocumentBson() throws Exception {

		IndexedDocBuilder docBuilder = new IndexedDocBuilder();
		docBuilder.addField("title", "Magic Java Beans");
		docBuilder.addField("issn", "4321-4321");
		docBuilder.addFacet("issn", "4321-4321");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		DBObject dbObject = new BasicDBObject();
		dbObject.put("someKey", "someValue");
		dbObject.put("other key", "other value");

		Store s = new Store("myid222", MY_INDEX_NAME);
		s.setIndexedDocument(indexedDoc);

		ResultDocBuilder resultDocumentBuilder = new ResultDocBuilder();
		resultDocumentBuilder.setDocument(dbObject);
		resultDocumentBuilder.setCompressed(true);
		s.setResultDocument(resultDocumentBuilder);

		lumongoWorkPool.store(s);

		Store s1 = new Store("myid2222", MY_INDEX_NAME);
		s1.setIndexedDocument(indexedDoc);

		ResultDocBuilder resultDocumentBuilder1 = new ResultDocBuilder();
		resultDocumentBuilder1.setDocument(dbObject);
		resultDocumentBuilder1.addMetaData("test1", "val1");
		resultDocumentBuilder1.addMetaData("test2", "val2");

		s1.setResultDocument(resultDocumentBuilder1);

		lumongoWorkPool.store(s1);

	}

	public void storeDocumentBinary() throws Exception {
		IndexedDocBuilder docBuilder = new IndexedDocBuilder();
		docBuilder.addField("title", "Another great and special book");
		docBuilder.addField("issn", "1111-1111");
		docBuilder.addFacet("issn", "1111-1111");
		LMDoc indexedDoc = docBuilder.getIndexedDoc();

		byte[] bytes = new byte[] { 1, 2, 3 };
		Store s = new Store("myid333", MY_INDEX_NAME);
		s.setIndexedDocument(indexedDoc);
		ResultDocBuilder resultDocumentBuilder = new ResultDocBuilder();
		resultDocumentBuilder.setDocument(bytes);

		lumongoWorkPool.store(s);

		Store s1 = new Store("myid3333", MY_INDEX_NAME);
		s1.setIndexedDocument(indexedDoc);

		ResultDocBuilder resultDocumentBuilder1 = new ResultDocBuilder();
		resultDocumentBuilder1.setDocument(bytes);
		resultDocumentBuilder1.addMetaData("test1", "val1");
		resultDocumentBuilder1.addMetaData("test2", "val2");

		s1.setResultDocument(resultDocumentBuilder1);

		lumongoWorkPool.store(s1);
	}

	public void fetchDocumentText() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid111", MY_INDEX_NAME);

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			String text = fetchResult.getDocumentAsUtf8();
			System.out.println(text);
		}

		FetchDocument fetchDocument1 = new FetchDocument("myid1111", MY_INDEX_NAME);

		FetchResult fetchResult1 = lumongoWorkPool.fetch(fetchDocument1);

		if (fetchResult1.hasResultDocument()) {
			String text = fetchResult1.getDocumentAsUtf8();
			System.out.println(text);

			Map<String, String> meta = fetchResult1.getMeta();
			System.out.println(meta);
		}
	}

	public void fetchDocumentBson() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid222", MY_INDEX_NAME);

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			DBObject object = fetchResult.getDocumentAsBson();
			System.out.println(object);
		}

		FetchDocument fetchDocument1 = new FetchDocument("myid2222", MY_INDEX_NAME);

		FetchResult fetchResult1 = lumongoWorkPool.fetch(fetchDocument1);

		if (fetchResult1.hasResultDocument()) {
			DBObject object = fetchResult1.getDocumentAsBson();
			System.out.println(object);

			Map<String, String> meta = fetchResult1.getMeta();
			System.out.println(meta);
		}
	}

	public void fetchDocumentBinary() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid333", MY_INDEX_NAME);

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);

		if (fetchResult.hasResultDocument()) {
			byte[] bytes = fetchResult.getDocumentAsBytes();
			System.out.println(Arrays.toString(bytes));
		}

		FetchDocument fetchDocument1 = new FetchDocument("myid3333", MY_INDEX_NAME);

		FetchResult fetchResult1 = lumongoWorkPool.fetch(fetchDocument1);

		if (fetchResult1.hasResultDocument()) {
			byte[] bytes = fetchResult1.getDocumentAsBytes();
			System.out.println(Arrays.toString(bytes));

			Map<String, String> meta = fetchResult1.getMeta();
			System.out.println(meta);
		}
	}

	public void fetchDocumentMeta() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid123", MY_INDEX_NAME);

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
		String[] indexes = new String[] { MY_INDEX_NAME, "myIndexName2" };
		String normalLuceneQuery = "issn:1234-1234 AND title:special";
		Query query = new Query(indexes, normalLuceneQuery, numberOfResults);
		query.setRealTime(false);
		QueryResult queryResult = lumongoWorkPool.query(query);

		long totalHits = queryResult.getTotalHits();

		System.out.println("Found <" + totalHits + "> hits");
		for (ScoredResult sr : queryResult.getResults()) {
			System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + ">");
		}

	}

	public void simpleQueryAsync() throws Exception {

		Query query = new Query(MY_INDEX_NAME, "issn:1234-1234 AND title:special", 10);

		ListenableFuture<QueryResult> resultFuture = lumongoWorkPool.queryAsync(query);

		Futures.addCallback(resultFuture, new FutureCallback<QueryResult>() {

			@Override
			public void onSuccess(QueryResult explosion) {

			}

			@Override
			public void onFailure(Throwable thrown) {

			}
		});
	}

	public void simpleQueryWithSort() throws Exception {
		int numberOfResults = 10;
		String normalLuceneQuery = "title:special";
		Query query = new Query(MY_INDEX_NAME, normalLuceneQuery, numberOfResults);
		query.addFieldSort("issn", Direction.ASCENDING);

		QueryResult queryResult = lumongoWorkPool.query(query);

		long totalHits = queryResult.getTotalHits();

		System.out.println("Found <" + totalHits + "> hits");
		for (ScoredResult sr : queryResult.getResults()) {
			System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + ">");
		}

	}

	public void queryWithBatchFetch() throws Exception {
		int numberOfResults = 10;
		String[] indexes = new String[] { MY_INDEX_NAME, "myIndexName2" };
		String normalLuceneQuery = "issn:1234-1234 AND title:special";
		Query query = new Query(indexes, normalLuceneQuery, numberOfResults);

		QueryResult queryResult = lumongoWorkPool.query(query);

		List<ScoredResult> scoredResults = queryResult.getResults();

		BatchFetch batchFetch = new BatchFetch();
		batchFetch.addFetchDocumentsFromResults(scoredResults);

		BatchFetchResult batchFetchResult = lumongoWorkPool.batchFetch(batchFetch);

		@SuppressWarnings("unused")
		List<FetchResult> results = batchFetchResult.getFetchResults();

	}

	public void pagingQuery() throws Exception {
		int numberOfResults = 2;
		String normalLuceneQuery = "issn:1234-1234 AND title:special";

		String[] indexes = new String[] { MY_INDEX_NAME, "myIndexName2" };
		Query query = new Query(indexes, normalLuceneQuery, numberOfResults);

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
		String[] indexes = new String[] { MY_INDEX_NAME, "myIndexName2" };

		Query query = new Query(indexes, "title:special", 0);
		int maxFacets = 30;
		query.addCountRequest("issn", maxFacets);

		QueryResult queryResult = lumongoWorkPool.query(query);
		for (FacetCount fc : queryResult.getFacetCounts()) {
			System.out.println("Facet <" + fc.getFacet() + "> with count <" + fc.getCount() + ">");
		}

	}

	public void drillDownQuery() throws Exception {
		Query query = new Query(MY_INDEX_NAME, "title:special", 0);
		query.addDrillDown(Query.drillDownfromParts("issn", "1111-1111"));
		QueryResult queryResult = lumongoWorkPool.query(query);
		for (FacetCount fc : queryResult.getFacetCounts()) {
			System.out.println("Facet <" + fc.getFacet() + "> with count <" + fc.getCount() + ">");
		}
	}

	public void getCount() throws Exception {
		GetNumberOfDocsResult result = lumongoWorkPool.getNumberOfDocs(MY_INDEX_NAME);
		System.out.println(result.getNumberOfDocs());
	}

	public void storeAssociated() throws Exception {
		String uniqueId = "myid123";
		String indexName = MY_INDEX_NAME;
		String filename = "myfile2";

		AssociatedBuilder associatedBuilder = new AssociatedBuilder();
		associatedBuilder.setFilename(filename);
		associatedBuilder.setCompressed(false);
		associatedBuilder.setDocument("Some Text3");
		associatedBuilder.addMetaData("mydata", "myvalue2");
		associatedBuilder.addMetaData("sometypeinfo", "text file2");

		Store s = new Store(uniqueId, indexName);
		s.addAssociatedDocument(associatedBuilder);

		lumongoWorkPool.store(s);
	}

	public void fetchAssociated() throws Exception {
		String uniqueId = "myid123";
		String filename = "myfile2";

		FetchAssociated fetchAssociated = new FetchAssociated(uniqueId, MY_INDEX_NAME, filename);

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchAssociated);

		if (fetchResult.getAssociatedDocumentCount() != 0) {
			AssociatedResult ad = fetchResult.getAssociatedDocument(0);
			String text = ad.getDocumentAsUtf8();
			System.out.println(text);
		}

	}

	public void fetchAllAssociated() throws Exception {
		String uniqueId = "myid123";

		FetchAllAssociated fetchAssociated = new FetchAllAssociated(uniqueId, MY_INDEX_NAME);

		FetchResult fetchResult = lumongoWorkPool.fetch(fetchAssociated);

		for (AssociatedResult ad : fetchResult.getAssociatedDocuments()) {
			String text = ad.getDocumentAsUtf8();
			System.out.println(text);
		}

	}

	public void storeLargeAssociated() throws Exception {
		String uniqueId = "myid333";
		String filename = "myfilename";

		StoreLargeAssociated storeLargeAssociated = new StoreLargeAssociated(uniqueId, filename, new File("/tmp/myFile"));

		lumongoWorkPool.storeLargeAssociated(storeLargeAssociated);

	}

	public void fetchLargeAssociated() throws Exception {
		String uniqueId = "myid333";
		String filename = "myfilename";

		FetchLargeAssociated fetchLargeAssociated = new FetchLargeAssociated(uniqueId, filename, new File("/tmp/myFetchedFile"));
		lumongoWorkPool.fetchLargeAssociated(fetchLargeAssociated);

	}

	public void deleteFromIndex() throws Exception {
		DeleteFromIndex deleteFromIndex = new DeleteFromIndex("myid111", MY_INDEX_NAME);
		lumongoWorkPool.delete(deleteFromIndex);
	}

	public void deleteFull() throws Exception {
		DeleteFull deleteFull = new DeleteFull("myid123", MY_INDEX_NAME);
		lumongoWorkPool.delete(deleteFull);
	}

	public void deleteSingleAssociated() throws Exception {
		DeleteAssociated deleteAssociated = new DeleteAssociated("myid123", MY_INDEX_NAME, "myfile2");
		lumongoWorkPool.delete(deleteAssociated);
	}

	public void deleteAllAssociated() throws Exception {
		DeleteAllAssociated deleteAllAssociated = new DeleteAllAssociated("myid123", MY_INDEX_NAME);
		lumongoWorkPool.delete(deleteAllAssociated);
	}

	public void getFields() throws Exception {
		GetFieldsResult result = lumongoWorkPool.getFields(new GetFields(MY_INDEX_NAME));
		System.out.println(result.getFieldNames());
	}

	public void getTerms() throws Exception {
		GetTermsResult getTermsResult = lumongoWorkPool.getAllTerms(new GetAllTerms(MY_INDEX_NAME, "title"));
		for (Term term : getTermsResult.getTerms()) {
			System.out.println(term.getValue() + ": " + term.getDocFreq());
		}

	}

	public void getIndexes() throws Exception {
		GetIndexesResult getIndexesResult = lumongoWorkPool.getIndexes();
		System.out.println(getIndexesResult.getIndexNames());

	}

	public void getDocumentCount() throws Exception {
		GetNumberOfDocsResult getNumberOfDocsResult = lumongoWorkPool.getNumberOfDocs(new GetNumberOfDocs(MY_INDEX_NAME));
		System.out.println(getNumberOfDocsResult.getNumberOfDocs());
	}

	public void getMembers() throws Exception {
		GetMembersResult getMembersResult = lumongoWorkPool.getMembers();
		for (LMMember member : getMembersResult.getMembers()) {
			System.out.println(member);
		}
	}

	public static void main(String[] args) throws Exception {
		ApiTest apiTest = new ApiTest();
		apiTest.startClient();
		try {
			apiTest.updateMembers();

			//apiTest.getMembers();

			apiTest.deleteIndex(MY_INDEX_NAME);
			apiTest.deleteIndex("myIndexName2");
			//apiTest.createIndex();
			//apiTest.updateIndex();

			apiTest.createOrUpdateIndex(MY_INDEX_NAME);
			apiTest.createOrUpdateIndex("myIndexName2");

			apiTest.storeDocumentText(MY_INDEX_NAME, "myid555");
			apiTest.storeDocumentText("myIndexName2", "myid666");

			/*
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

			apiTest.getFields();
			apiTest.getTerms();

			apiTest.getIndexes();
			apiTest.getDocumentCount();
			 */

			apiTest.simpleQueryWithSort();

			apiTest.facetQuery();

		} catch (Exception e) {
			System.err.println(e);
		} finally {
			apiTest.stopClient();
		}
	}

}
