package org.lumongo.test.cluster;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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
import org.lumongo.cluster.message.Lumongo.FacetAs.LMFacetType;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMMember;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.Term;
import org.lumongo.doc.AssociatedBuilder;
import org.lumongo.doc.ResultDocBuilder;
import org.lumongo.fields.FieldConfigBuilder;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ApiTest {
	public static final String MY_INDEX_NAME = SingleNodeTest.MY_TEST_INDEX;
	public static final String MY_INDEX_NAME2 = "myIndexName2";
	
	private LumongoWorkPool lumongoWorkPool;
	
	public void startClient() throws Exception {
		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
		lumongoPoolConfig.addMember("localhost");
		lumongoPoolConfig.setDefaultRetries(0);
		lumongoPoolConfig.setMaxConnections(8);
		lumongoPoolConfig.setMaxIdle(8);
		lumongoPoolConfig.setCompressedConnection(false);
		lumongoPoolConfig.setPoolName(null);
		lumongoPoolConfig.setMemberUpdateEnabled(true);
		lumongoPoolConfig.setMemberUpdateInterval(10000);
		lumongoPoolConfig.setRoutingEnabled(true);
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
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title").indexAs(LMAnalyzer.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("issn").indexAs(LMAnalyzer.LC_KEYWORD).facetAs(LMFacetType.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("an").indexAs(LMAnalyzer.NUMERIC_INT));
		
		CreateIndex createIndex = new CreateIndex(MY_INDEX_NAME, numberOfSegments, uniqueIdField, indexConfig);
		lumongoWorkPool.createIndex(createIndex);
	}
	
	public void updateIndex() throws Exception {
		
		String defaultSearchField = "abstract";
		IndexConfig indexConfig = new IndexConfig(defaultSearchField);
		
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title").indexAs(LMAnalyzer.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("issn").indexAs(LMAnalyzer.LC_KEYWORD).facetAs(LMFacetType.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("an").indexAs(LMAnalyzer.NUMERIC_INT));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("abstract").indexAs(LMAnalyzer.STANDARD));
		
		UpdateIndex updateIndex = new UpdateIndex(MY_INDEX_NAME, indexConfig);
		lumongoWorkPool.updateIndex(updateIndex);
	}
	
	public void createOrUpdateIndex(String indexName) throws Exception {
		String defaultSearchField = "abstract";
		int numberOfSegments = 16;
		String uniqueIdField = "uid";
		
		IndexConfig indexConfig = new IndexConfig(defaultSearchField);
		
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title").indexAs(LMAnalyzer.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("issn").indexAs(LMAnalyzer.LC_KEYWORD).facetAs(LMFacetType.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("an").indexAs(LMAnalyzer.NUMERIC_INT));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("abstract").indexAs(LMAnalyzer.STANDARD));
		
		CreateOrUpdateIndex createOrUpdateIndex = new CreateOrUpdateIndex(indexName, numberOfSegments, uniqueIdField, indexConfig);
		CreateOrUpdateIndexResult result = lumongoWorkPool.createOrUpdateIndex(createOrUpdateIndex);
		System.out.println(result.isNewIndex());
		System.out.println(result.isUpdatedIndex());
	}
	
	public void storeDocumentBson() throws Exception {
		
		DBObject dbObject = new BasicDBObject();
		dbObject.put("title", "Magic Java Beans");
		dbObject.put("issn", "4321-4321");
		
		Store s = new Store("myid222", MY_INDEX_NAME);
		
		ResultDocBuilder resultDocumentBuilder = new ResultDocBuilder();
		resultDocumentBuilder.setDocument(dbObject);
		s.setResultDocument(resultDocumentBuilder);
		
		lumongoWorkPool.store(s);
		
		//with same with meta
		Store s1 = new Store("myid2222", MY_INDEX_NAME);
		
		ResultDocBuilder resultDocumentBuilder1 = new ResultDocBuilder();
		resultDocumentBuilder1.setDocument(dbObject);
		resultDocumentBuilder1.addMetaData("testFieldExtraction", "val1");
		resultDocumentBuilder1.addMetaData("test2", "val2");
		
		s1.setResultDocument(resultDocumentBuilder1);
		
		lumongoWorkPool.store(s1);
		
	}
	
	public void fetchDocumentBson() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid222", MY_INDEX_NAME);
		
		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);
		
		if (fetchResult.hasResultDocument()) {
			DBObject object = fetchResult.getDocument();
			System.out.println(object);
		}
		
		FetchDocument fetchDocument1 = new FetchDocument("myid2222", MY_INDEX_NAME);
		
		FetchResult fetchResult1 = lumongoWorkPool.fetch(fetchDocument1);
		
		if (fetchResult1.hasResultDocument()) {
			DBObject object = fetchResult1.getDocument();
			System.out.println(object);
			
			Map<String, String> meta = fetchResult1.getMeta();
			System.out.println(meta);
		}
	}
	
	public void fetchDocumentMeta() throws Exception {
		FetchDocument fetchDocument = new FetchDocument("myid123", MY_INDEX_NAME);
		
		FetchResult fetchResult = lumongoWorkPool.fetch(fetchDocument);
		
		if (fetchResult.hasResultDocument()) {
			
			Map<String, String> meta = fetchResult.getMeta();
			System.out.println(meta);
		}
		
	}
	
	public void simpleQuery() throws Exception {
		int numberOfResults = 10;
		String[] indexes = new String[] { MY_INDEX_NAME, MY_INDEX_NAME2 };
		String normalLuceneQuery = "issn:1234-1234 AND title:special";
		Query query = new Query(indexes, normalLuceneQuery, numberOfResults);
		QueryResult queryResult = lumongoWorkPool.query(query);
		
		long totalHits = queryResult.getTotalHits();
		
		System.out.println("Found <" + totalHits + "> hits");
		for (ScoredResult sr : queryResult.getResults()) {
			System.out.println("Matching document <" + sr.getUniqueId() + "> with score <" + sr.getScore() + ">");
		}
		
	}
	
	public void moreComplexQuery() throws Exception {
		int numberOfResults = 10;
		
		Query query = new Query(Arrays.asList(MY_INDEX_NAME, MY_INDEX_NAME2), "cancer cure", numberOfResults);
		query.addQueryField("abstract");
		query.addQueryField("title");
		query.addFilterQuery("title:special");
		query.addFilterQuery("issn:1234-1234");
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
		String[] indexes = new String[] { MY_INDEX_NAME, MY_INDEX_NAME2 };
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
		
		String[] indexes = new String[] { MY_INDEX_NAME, MY_INDEX_NAME2 };
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
		String[] indexes = new String[] { MY_INDEX_NAME, MY_INDEX_NAME2 };
		
		Query query = new Query(indexes, "title:special", 0);
		int maxFacets = 30;
		query.addCountRequest(maxFacets, "issn");
		
		QueryResult queryResult = lumongoWorkPool.query(query);
		for (FacetCount fc : queryResult.getFacetCounts("issn")) {
			System.out.println("Facet <" + fc.getFacet() + "> with count <" + fc.getCount() + ">");
		}
		
	}
	
	public void drillDownQuery() throws Exception {
		Query query = new Query(MY_INDEX_NAME, "title:special", 0);
		query.addDrillDown("issn", "1111-1111");
		QueryResult queryResult = lumongoWorkPool.query(query);
		for (FacetCount fc : queryResult.getFacetCounts("issn")) {
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
		String indexName = MY_INDEX_NAME;
		
		StoreLargeAssociated storeLargeAssociated = new StoreLargeAssociated(uniqueId, indexName, filename, new File("/tmp/myFile"));
		
		lumongoWorkPool.storeLargeAssociated(storeLargeAssociated);
		
	}
	
	public void fetchLargeAssociated() throws Exception {
		String uniqueId = "myid333";
		String filename = "myfilename";
		String indexName = MY_INDEX_NAME;
		
		FetchLargeAssociated fetchLargeAssociated = new FetchLargeAssociated(uniqueId, indexName, filename, new File("/tmp/myFetchedFile"));
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
			
			apiTest.getMembers();
			
			//apiTest.deleteIndex(MY_INDEX_NAME);
			//apiTest.deleteIndex("myIndexName2");
			//apiTest.createIndex();
			//apiTest.updateIndex();
			
			apiTest.createOrUpdateIndex(MY_INDEX_NAME);
			apiTest.createOrUpdateIndex(MY_INDEX_NAME2);
			
			apiTest.storeDocumentBson();
			apiTest.fetchDocumentBson();
			
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
			
			apiTest.simpleQueryWithSort();
			
			apiTest.facetQuery();
			
		}
		catch (Exception e) {
			System.err.println(e);
		}
		finally {
			apiTest.stopClient();
		}
	}
	
}
