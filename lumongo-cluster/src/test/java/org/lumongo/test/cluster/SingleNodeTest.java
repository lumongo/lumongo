package org.lumongo.test.cluster;

import org.lumongo.client.command.DeleteAllAssociated;
import org.lumongo.client.command.DeleteAssociated;
import org.lumongo.client.command.DeleteFull;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.FetchDocumentAndAssociated;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.doc.AssociatedBuilder;
import org.lumongo.doc.IndexedDocBuilder;
import org.lumongo.doc.ResultDocBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SingleNodeTest {
	private static final String MY_TEST_INDEX = "myTestIndex";
	
	private static final String FACET_TEST_INDEX = "facetTestIndex";
	
	@SuppressWarnings("unused")
	private static Logger log = Logger.getLogger(SingleNodeTest.class);
	
	private LumongoWorkPool lumongoWorkPool;
	
	@Test(
		groups = { "init" })
	public void createIndexTest() throws Exception {
		
		lumongoWorkPool = SetupSuite.getLumongoWorkPool();
		
		String defaultSearchField = "title";
		IndexConfig indexConfig = new IndexConfig(defaultSearchField);
		indexConfig.setDefaultAnalyzer(LMAnalyzer.KEYWORD);
		indexConfig.setSegmentTolerance(0.05);
		indexConfig.setFieldAnalyzer("title", LMAnalyzer.STANDARD);
		indexConfig.setFieldAnalyzer("issn", LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("uid", LMAnalyzer.LC_KEYWORD);
		indexConfig.setFieldAnalyzer("an", LMAnalyzer.NUMERIC_INT);
		
		lumongoWorkPool.createIndex(MY_TEST_INDEX, 16, "uid", indexConfig);
		lumongoWorkPool.createIndex(FACET_TEST_INDEX, 16, "uid", indexConfig, true);
	}
	
	@Test(
		groups = { "facet" },
		dependsOnGroups = { "init" })
	public void facetTest() throws Exception {
		final int COUNT_PER_ISSN = 100;
		final String uniqueIdPrefix = "myId-";
		
		final String[] issns = new String[] { "1234-1234", "3333-1234", "1234-5555", "1234-4444", "2222-2222" };
		int totalRecords = COUNT_PER_ISSN * issns.length;
		
		int id = 0;
		{
			for (String issn : issns) {
				for (int i = 0; i < COUNT_PER_ISSN; i++) {
					boolean half = (i % 2 == 0);
					boolean tenth = (i % 10 == 0);
					
					id++;
					
					String uniqueId = uniqueIdPrefix + id;
					
					IndexedDocBuilder docBuilder = new IndexedDocBuilder();
					docBuilder.addField("issn", issn);
					docBuilder.addField("title", "Facet Userguide");
					docBuilder.addFacet("issn", issn);
					
					if (half) { // 1/2 of input
						docBuilder.addFacet("country", "US");
					}
					else { // 1/2 of input
						docBuilder.addFacet("country", "France");
					}
					
					if (tenth) { // 1/10 of input
						docBuilder.addFacet("date", "2014", "10", "4");
					}
					else if (half) { // 2/5 of input
						docBuilder.addFacet("date", "2013", "9", "4");
					}
					else { // 1/2 of input
						docBuilder.addFacet("date", "2013", "8", "4");
					}
					
					LMDoc indexedDoc = docBuilder.getIndexedDoc();
					
					String xml = "<sampleXML>" + i + "</sampleXML>";
					
					Store s = new Store(uniqueId, FACET_TEST_INDEX);
					s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(xml).setCompressed(half));
					s.setIndexedDocument(indexedDoc);
					
					lumongoWorkPool.store(s);
				}
			}
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest("issn", 30);
			QueryResult qr = lumongoWorkPool.query(q);
			
			Assert.assertEquals(qr.getTotalHits(), totalRecords, "Total record count not " + totalRecords);
			
			Assert.assertEquals(qr.getFacetCounts("issn").size(), issns.length, "Total facets not " + issns.length);
			for (FacetCount fc : qr.getFacetCounts("issn")) {
				Assert.assertEquals(fc.getCount(), COUNT_PER_ISSN, "Count for facet <" + fc.getFacet() + "> not <" + COUNT_PER_ISSN + ">");
			}
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2014");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			Assert.assertEquals(qr.getTotalHits(), totalRecords / 10, "Total record count after drill down not " + totalRecords / 10);
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2013", "9");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			Assert.assertEquals(qr.getTotalHits(), (totalRecords * 2) / 5, "Total record count after drill down not " + (totalRecords * 2) / 5);
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2013", "8");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			Assert.assertEquals(qr.getTotalHits(), totalRecords / 2, "Total record count after drill down not " + totalRecords / 2);
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			Assert.assertEquals(qr.getTotalHits(), COUNT_PER_ISSN, "Total record count after drill down not " + COUNT_PER_ISSN);
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234").addDrillDown("issn", "3333-1234");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			Assert.assertEquals(qr.getTotalHits(), COUNT_PER_ISSN * 2, "Total record count after drill down not " + (COUNT_PER_ISSN * 2));
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234").addDrillDown("country", "France");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			Assert.assertEquals(qr.getTotalHits(), COUNT_PER_ISSN / 2, "Total record count after drill down not " + (COUNT_PER_ISSN / 2));
			Assert.assertEquals(qr.getTotalHits(), COUNT_PER_ISSN / 2, "Total record count after drill down not " + (COUNT_PER_ISSN / 2));
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10);
			q.addDrillDown("issn", "1234-1234").addDrillDown("country", "France");
			q.addCountRequest("issn");
			q.setDrillSideways(true);
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			Assert.assertEquals(qr.getTotalHits(), COUNT_PER_ISSN / 2, "Total record count after drill down not " + (COUNT_PER_ISSN / 2));
			Assert.assertEquals(qr.getFacetCounts("issn").size(), issns.length, "Number of issn facets not equal " + issns.length);
			
			q.setDrillSideways(false);
			qr = lumongoWorkPool.query(q);
			Assert.assertEquals(qr.getFacetCounts("issn").size(), 1, "Number of issn facets not equal " + 1);
		}
		
	}
	
	@Test(
		groups = { "first" },
		dependsOnGroups = { "init" })
	public void bulkTest() throws Exception {
		
		final int DOCUMENTS_LOADED = 5;
		final String uniqueIdPrefix = "someUniqueId-";
		{
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				
				IndexedDocBuilder docBuilder = new IndexedDocBuilder();
				docBuilder.addField("issn", "1333-1333");
				docBuilder.addField("title", "Search and Storage");
				LMDoc indexedDoc = docBuilder.getIndexedDoc();
				
				boolean compressed = (i % 2 == 0);
				
				String xml = "<sampleXML>random xml</sampleXML>";
				
				Store s = new Store(uniqueId, MY_TEST_INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(xml).setCompressed(compressed));
				s.setIndexedDocument(indexedDoc);
				lumongoWorkPool.store(s);
			}
			
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				
				IndexedDocBuilder docBuilder = new IndexedDocBuilder();
				docBuilder.addField("issn", "1234-1234");
				docBuilder.addField("title", "Distributed Search and Storage System");
				docBuilder.addField("an", i);
				LMDoc indexedDoc = docBuilder.getIndexedDoc();
				
				boolean compressed = (i % 2 == 0);
				
				String xml = "<sampleXML>" + i + "</sampleXML>";
				
				Store s = new Store(uniqueId, MY_TEST_INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(xml).setCompressed(compressed));
				s.setIndexedDocument(indexedDoc);
				lumongoWorkPool.store(s);
			}
		}
		{
			QueryResult qr = null;
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "an:3", 10));
			Assert.assertEquals(qr.getTotalHits(), 1, "Total hits is not 1");
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "an:[1 TO 3]", 10));
			Assert.assertEquals(qr.getTotalHits(), 3, "Total hits is not 3");
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "an:{1 TO 3}", 10));
			Assert.assertEquals(qr.getTotalHits(), 1, "Total hits is not 1");
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "title:distributed", 300));
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "title:distributed", 100));
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "distributed", 20));
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "issn:1234-1234", 20));
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "title:cluster", 10));
			Assert.assertEquals(qr.getTotalHits(), 0, "Total hits is not 0");
			
		}
		
		{
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				
				FetchResult response = lumongoWorkPool.fetch(new FetchDocument(uniqueId, MY_TEST_INDEX));
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				String recordText = response.getDocumentAsUtf8();
				System.out.println("\n:" + recordText + ":\n");
				Assert.assertTrue(recordText.equals("<sampleXML>" + i + "</sampleXML>"), "Document contents is invalid for <" + uniqueId + ">");
			}
		}
	}
	
	@Test(
		groups = { "first" },
		dependsOnGroups = { "init" })
	public void bsonTest() throws Exception {
		
		String uniqueId = "bsonTestObjectId";
		{
			IndexedDocBuilder docBuilder = new IndexedDocBuilder();
			docBuilder.addField("issn", "4321-4321");
			docBuilder.addField("title", "Magic Java Beans");
			docBuilder.addField("eissn", "3333-3333");
			LMDoc indexedDoc = docBuilder.getIndexedDoc();
			
			DBObject dbObject = new BasicDBObject();
			dbObject.put("someKey", "someValue");
			dbObject.put("other key", "other value");
			
			Store s = new Store(uniqueId, MY_TEST_INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(dbObject)).setIndexedDocument(indexedDoc);
			lumongoWorkPool.store(s);
		}
		
		{
			FetchResult response = lumongoWorkPool.fetch(new FetchDocument(uniqueId, MY_TEST_INDEX));
			Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
			DBObject dbObject = response.getDocumentAsBson();
			Assert.assertEquals(dbObject.get("someKey"), "someValue", "BSON object is missing field");
			Assert.assertEquals(dbObject.get("other key"), "other value", "BSON object is missing field");
		}
		
	}
	
	@Test(
		groups = { "first" },
		dependsOnGroups = { "init" })
	public void associatedTest() throws Exception {
		
		String uniqueId = "id3333";
		{
			{
				IndexedDocBuilder docBuilder = new IndexedDocBuilder();
				docBuilder.addField("issn", "6666-6666");
				docBuilder.addField("title", "More Magic Java Beans");
				docBuilder.addField("eissn", 2222 - 1111);
				LMDoc indexedDoc = docBuilder.getIndexedDoc();
				
				DBObject dbObject = new BasicDBObject();
				dbObject.put("key1", "val1");
				dbObject.put("key2", "val2");
				
				AssociatedBuilder associatedBuilder = new AssociatedBuilder();
				associatedBuilder.setFilename("myfile");
				associatedBuilder.setCompressed(true);
				associatedBuilder.setDocument("Some Text");
				associatedBuilder.addMetaData("mydata", "myvalue");
				associatedBuilder.addMetaData("sometypeinfo", "text file");
				
				Store s = new Store(uniqueId, MY_TEST_INDEX);
				s.setIndexedDocument(indexedDoc);
				s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(dbObject));
				s.addAssociatedDocument(associatedBuilder);
				
				lumongoWorkPool.store(s);
				
			}
			{
				
				AssociatedBuilder associatedBuilder = new AssociatedBuilder();
				associatedBuilder.setFilename("myfile2");
				associatedBuilder.setCompressed(false);
				associatedBuilder.setDocument("Some Other Text");
				associatedBuilder.addMetaData("mydata", "myvalue 2");
				associatedBuilder.addMetaData("sometypeinfo", "text file");
				
				Store s = new Store(uniqueId, MY_TEST_INDEX);
				s.addAssociatedDocument(associatedBuilder);
				lumongoWorkPool.store(s);
			}
			{
				AssociatedBuilder associatedBuilder = new AssociatedBuilder();
				associatedBuilder.setFilename("filef");
				associatedBuilder.setCompressed(true);
				associatedBuilder.setDocument("Some Other Text");
				associatedBuilder.addMetaData("stuff", "mystuff");
				
				Store s = new Store(uniqueId, MY_TEST_INDEX);
				s.addAssociatedDocument(associatedBuilder);
				lumongoWorkPool.store(s);
			}
		}
		{
			{
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX, true));
				
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				DBObject dbObject = response.getDocumentAsBson();
				;
				Assert.assertEquals(dbObject.get("key1"), "val1", "BSON object is missing field");
				Assert.assertEquals(dbObject.get("key2"), "val2", "BSON object is missing field");
				
				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expected 3 associated documents");
				Assert.assertTrue(!response.getAssociatedDocument(0).hasDocument(), "Associated Document should be meta only");
				Assert.assertTrue(!response.getAssociatedDocument(1).hasDocument(), "Associated Document should be meta only");
				Assert.assertTrue(!response.getAssociatedDocument(2).hasDocument(), "Associated Document should be meta only");
			}
			{
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				DBObject dbObject = response.getDocumentAsBson();
				Assert.assertEquals(dbObject.get("key1"), "val1", "BSON object is missing field");
				// Assert.assertEquals(dbObject.get("key2"), "val2", "BSON object is missing field");
				
				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expected 3 associated documents");
				Assert.assertTrue(response.getAssociatedDocument(0).hasDocument(), "Associated document does not exist");
				Assert.assertTrue(response.getAssociatedDocument(1).hasDocument(), "Associated document does not exist");
				Assert.assertTrue(response.getAssociatedDocument(2).hasDocument(), "Associated document does not exist");
				
			}
		}
	}
	
	@Test(
		groups = { "next" },
		dependsOnGroups = { "first" })
	public void deleteTest() throws Exception {
		{
			String uniqueIdToDelete = "someUniqueId-" + 4;
			
			QueryResult qr = null;
			FetchResult fr = null;
			
			fr = lumongoWorkPool.fetch(new FetchDocument(uniqueIdToDelete, MY_TEST_INDEX));
			Assert.assertTrue(fr.hasResultDocument(), "Document is missing raw document before delete");
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "uid" + ":" + uniqueIdToDelete, 10));
			Assert.assertEquals(qr.getTotalHits(), 1, "Total hits is not 1 before delete");
			
			lumongoWorkPool.delete(new DeleteFull(uniqueIdToDelete, MY_TEST_INDEX));
			
			fr = lumongoWorkPool.fetch(new FetchDocument(uniqueIdToDelete, MY_TEST_INDEX));
			Assert.assertTrue(!fr.hasResultDocument(), "Document has raw document after delete");
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "uid" + ":" + uniqueIdToDelete, 10));
			Assert.assertEquals(qr.getTotalHits(), 0, "Total hits is not 0 after delete");
		}
		
		{
			String uniqueId = "id3333";
			String fileName = "myfile2";
			{
				
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expecting 3 associated documents");
			}
			
			{
				lumongoWorkPool.delete(new DeleteAssociated(uniqueId, MY_TEST_INDEX, fileName));
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 2, "Expecting 2 associated document");
			}
			
			{
				lumongoWorkPool.delete(new DeleteAllAssociated(uniqueId, MY_TEST_INDEX));
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 0, "Expecting 0 associated documents");
				Assert.assertTrue(response.hasResultDocument(), "Expecting raw document");
				
			}
			
			{
				lumongoWorkPool.delete(new DeleteFull(uniqueId, MY_TEST_INDEX));
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 0, "Expecting 0 associated document");
				Assert.assertTrue(!response.hasResultDocument(), "Expecting no raw document");
				
			}
			
		}
	}
	
	@Test(
		groups = { "last" },
		dependsOnGroups = { "next" })
	public void apiUsage() throws Exception {
		{
			IndexedDocBuilder docBuilder = new IndexedDocBuilder();
			docBuilder.addField("issn", "4444-1111");
			docBuilder.addField("title", "A really special title to search");
			LMDoc indexedDoc = docBuilder.getIndexedDoc();
			
			String uniqueId = "myid123";
			Store s = new Store(uniqueId, MY_TEST_INDEX);
			s.setIndexedDocument(indexedDoc);
			
			String xml = "<sampleXML></sampleXML>";
			
			s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(xml).setCompressed(true));
			
			lumongoWorkPool.store(s);
		}
		{
			int numberOfResults = 10;
			String normalLuceneQuery = "issn:1234-1234 AND title:special";
			
			Query q = new Query(MY_TEST_INDEX, normalLuceneQuery, numberOfResults);
			QueryResult qr = lumongoWorkPool.query(q);
			long totalHits = qr.getTotalHits();
			
			for (ScoredResult sr : qr.getResults()) {
				String uniqueId = sr.getUniqueId();
				float score = sr.getScore();
				System.out.println("Matching document <" + uniqueId + "> with score <" + score + "> and timestamp <" + sr.getTimestamp() + ">");
			}
			
			System.out.println("Query <" + normalLuceneQuery + "> found <" + totalHits + "> total hits.  Fetched <" + qr.getResults().size() + "> documents.");
		}
		
		{
			int numberOfResults = 10;
			String normalLuceneQuery = "title:special";
			
			Query q = new Query(MY_TEST_INDEX, normalLuceneQuery, numberOfResults);
			
			QueryResult first = lumongoWorkPool.query(q);
			
			@SuppressWarnings("unused")
			QueryResult next = lumongoWorkPool.query(q.setLastResult(first));
			
		}
		
		{
			String uniqueIdToDelete = "someId";
			lumongoWorkPool.delete(new DeleteFull(uniqueIdToDelete, MY_TEST_INDEX));
		}
		
		{
			String uniqueId = "someId";
			FetchResult fr = lumongoWorkPool.fetch(new FetchDocument(uniqueId, MY_TEST_INDEX));
			if (!fr.hasResultDocument()) {
				System.out.println("Document: " + fr.getDocumentAsUtf8());
			}
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest("issn", 30);
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			for (FacetCount fc : qr.getFacetCounts("issn")) {
				System.out.println("Facet <" + fc.getFacet() + "> has <" + fc.getCount() + "> items");
			}
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn/1234-1234");
			
			@SuppressWarnings("unused")
			QueryResult qr = lumongoWorkPool.query(q);
			
		}
	}
	
	@Test(
		groups = { "drop" },
		dependsOnGroups = { "last", "facet" })
	public void dropIndex() throws Exception {
		GetIndexesResult gir = null;
		
		gir = lumongoWorkPool.getIndexes();
		Assert.assertEquals(gir.getIndexCount(), 2, "Expected two indexes");
		lumongoWorkPool.deleteIndex(new DeleteIndex(MY_TEST_INDEX));
		gir = lumongoWorkPool.getIndexes();
		Assert.assertEquals(gir.getIndexCount(), 1, "Expected one indexes");
	}
}
