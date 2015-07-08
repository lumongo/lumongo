package org.lumongo.test.cluster;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.lumongo.client.command.DeleteAllAssociated;
import org.lumongo.client.command.DeleteAssociated;
import org.lumongo.client.command.DeleteFull;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.FetchDocumentAndAssociated;
import org.lumongo.client.command.FetchLargeAssociated;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.FacetAs.LMFacetType;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.doc.AssociatedBuilder;
import org.lumongo.doc.ResultDocBuilder;
import org.lumongo.fields.FieldConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Date;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class SingleNodeTest extends ServerTestBase {
	public static final String MY_TEST_INDEX = "myTestIndex";
	
	public static final String FACET_TEST_INDEX = "facetTestIndex";

	@BeforeClass
	public void test01Start() throws Exception {
		startSuite(1);
	}
	
	@Test
	public void test02Init() throws Exception {

		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();
		
		String defaultSearchField = "title";
		IndexConfig indexConfig = new IndexConfig(defaultSearchField);
		
		indexConfig.setSegmentTolerance(0.05);
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title").indexAs(LMAnalyzer.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("issn").indexAs(LMAnalyzer.LC_KEYWORD).facetAs(LMFacetType.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("eissn").indexAs(LMAnalyzer.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("uid").indexAs(LMAnalyzer.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("an").indexAs(LMAnalyzer.NUMERIC_INT).sortAs("an", Lumongo.SortAs.SortType.NUMERIC_INT));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("country").indexAs(LMAnalyzer.LC_KEYWORD).facetAs(LMFacetType.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("date").indexAs(LMAnalyzer.DATE).facetAs(LMFacetType.DATE_YYYY_MM_DD));

		lumongoWorkPool.createIndex(MY_TEST_INDEX, 16, "uid", indexConfig);
		lumongoWorkPool.createIndex(FACET_TEST_INDEX, 1, "uid", indexConfig);
	}
	
	@Test
	public void test03Facet() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		final int COUNT_PER_ISSN = 10;
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
					
					DBObject object = new BasicDBObject();
					object.put("issn", issn);
					object.put("title", "Facet Userguide");
					
					if (half) { // 1/2 of input
						object.put("country", "US");
					}
					else { // 1/2 of input
						object.put("country", "France");
					}
					
					if (tenth) { // 1/10 of input
						Date d = (new DateTime(DateTimeZone.UTC)).withDate(2014, 10, 4).toDate();
						object.put("date", d);
					}
					else if (half) { // 2/5 of input
						Date d = (new DateTime(DateTimeZone.UTC)).withDate(2013, 9, 4).toDate();
						object.put("date", d);
					}
					else { // 1/2 of input
						Date d = (new DateTime(DateTimeZone.UTC)).withDate(2013, 8, 4).toDate();
						object.put("date", d);
					}
					
					Store s = new Store(uniqueId, FACET_TEST_INDEX);
					s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(object));
					
					lumongoWorkPool.store(s);
				}
			}
		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest(30, "issn");
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count not " + totalRecords, totalRecords, qr.getTotalHits());
			
			assertEquals("Total facets not " + issns.length, qr.getFacetCounts("issn").size(), issns.length);
			for (FacetCount fc : qr.getFacetCounts("issn")) {
				assertEquals("Count for facet <" + fc.getFacet() + "> not <" + COUNT_PER_ISSN + ">", COUNT_PER_ISSN, fc.getCount());
			}
			
		}
		
		{
			
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest(30, "date");
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count not " + totalRecords, totalRecords, qr.getTotalHits());

			for (@SuppressWarnings("unused") FacetCount fc : qr.getFacetCounts("date")) {
				System.out.println("Date: " + fc);
			}

			assertEquals("Total facets not " + 3, 3, qr.getFacetCounts("date").size());

		}

		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2014-10-04");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count after drill down not " + totalRecords / 10, totalRecords / 10, qr.getTotalHits());
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2013-09-04");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count after drill down not " + (totalRecords * 2) / 5, (totalRecords * 2) / 5, qr.getTotalHits());
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2013-08-04");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count after drill down not " + totalRecords / 2, totalRecords / 2, qr.getTotalHits());
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count after drill down not " + COUNT_PER_ISSN, COUNT_PER_ISSN, qr.getTotalHits());
			
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234").addDrillDown("issn", "3333-1234");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count after drill down not " + (COUNT_PER_ISSN * 2), COUNT_PER_ISSN * 2, qr.getTotalHits());
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234").addDrillDown("country", "France");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count after drill down not " + (COUNT_PER_ISSN / 2), COUNT_PER_ISSN / 2, qr.getTotalHits());
			assertEquals("Total record count after drill down not " + (COUNT_PER_ISSN / 2), COUNT_PER_ISSN / 2, qr.getTotalHits());
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10);
			q.addDrillDown("issn", "1234-1234").addDrillDown("country", "France");
			q.addCountRequest("issn");

			QueryResult qr = lumongoWorkPool.query(q);
			
			assertEquals("Total record count after drill down not " + (COUNT_PER_ISSN / 2), COUNT_PER_ISSN / 2, qr.getTotalHits());
			assertEquals("Number of issn facets not equal " + 1, 1, qr.getFacetCounts("issn").size());
			

		}
		
	}
	
	@Test
	public void test04Bulk() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		final int DOCUMENTS_LOADED = 5;
		final String uniqueIdPrefix = "someUniqueId-";
		{
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				
				DBObject object = new BasicDBObject();
				object.put("issn", "1333-1333");
				object.put("title", "Search and Storage");
				
				Store s = new Store(uniqueId, MY_TEST_INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(object));
				lumongoWorkPool.store(s);
			}
			
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				
				DBObject object = new BasicDBObject();
				object.put("issn", "1234-1234");
				object.put("title", "Distributed Search and Storage System");
				object.put("an", i);
				
				Store s = new Store(uniqueId, MY_TEST_INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(object));
				lumongoWorkPool.store(s);
			}
		}
		{
			QueryResult qr;
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "an:3", 10));
			assertEquals("Total hits is not 1", 1, qr.getTotalHits());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "an:[1 TO 3]", 10));
			assertEquals("Total hits is not 3", 3, qr.getTotalHits());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "an:{1 TO 3}", 10));
			assertEquals("Total hits is not 1", 1, qr.getTotalHits());

			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "an:[1 TO 5]", 10).addFieldSort("an"));
			assertEquals("Unique id does not match expected", "someUniqueId-1", qr.getResults().get(0).getUniqueId());

			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "an:[1 TO 4]", 10).addFieldSort("an", Lumongo.FieldSort.Direction.DESCENDING));
			assertEquals("Unique id does not match expected", "someUniqueId-4", qr.getResults().get(0).getUniqueId());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "title:distributed", 300));
			assertEquals("Total hits is not " + DOCUMENTS_LOADED, DOCUMENTS_LOADED, qr.getTotalHits());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "title:distributed", 100));
			assertEquals("Total hits is not " + DOCUMENTS_LOADED, DOCUMENTS_LOADED, qr.getTotalHits());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "distributed", 20));
			assertEquals("Total hits is not " + DOCUMENTS_LOADED, DOCUMENTS_LOADED, qr.getTotalHits());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "issn:1234-1234", 20));
			assertEquals("Total hits is not " + DOCUMENTS_LOADED, DOCUMENTS_LOADED, qr.getTotalHits());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "title:cluster", 10));
			assertEquals("Total hits is not 0", 0, qr.getTotalHits());
			
		}
		
		{
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				
				FetchResult response = lumongoWorkPool.fetch(new FetchDocument(uniqueId, MY_TEST_INDEX));
				assertTrue("Fetch failed for <" + uniqueId + ">", response.hasResultDocument());
				
			}
		}
	}
	
	@Test
	public void test05Bson() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		String uniqueId = "bsonTestObjectId";
		{
			
			DBObject dbObject = new BasicDBObject();
			dbObject.put("someKey", "someValue");
			dbObject.put("other key", "other value");
			dbObject.put("issn", "4321-4321");
			dbObject.put("title", "Magic Java Beans");
			dbObject.put("eissn", "3333-3333");
			
			Store s = new Store(uniqueId, MY_TEST_INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(dbObject));
			lumongoWorkPool.store(s);
		}
		
		{
			FetchResult response = lumongoWorkPool.fetch(new FetchDocument(uniqueId, MY_TEST_INDEX));
			assertTrue("Fetch failed for <" + uniqueId + ">", response.hasResultDocument());
			DBObject dbObject = response.getDocument();
			assertEquals("BSON object is missing field", "someValue", dbObject.get("someKey"));
			assertEquals("BSON object is missing field", "other value", dbObject.get("other key"));
		}
		
	}
	
	@Test
	public void test06AssociatedDocuments() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		String uniqueId = "id3333";
		{
			{
				DBObject dbObject = new BasicDBObject();
				dbObject.put("key1", "val1");
				dbObject.put("key2", "val2");
				dbObject.put("issn", "6666-6666");
				dbObject.put("title", "More Magic Java Beans");
				dbObject.put("eissn", 2222 - 1111);
				
				AssociatedBuilder associatedBuilder = new AssociatedBuilder();
				associatedBuilder.setFilename("myfile");
				associatedBuilder.setCompressed(true);
				associatedBuilder.setDocument("Some Text");
				associatedBuilder.addMetaData("mydata", "myvalue");
				associatedBuilder.addMetaData("sometypeinfo", "text file");
				
				Store s = new Store(uniqueId, MY_TEST_INDEX);
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

				assertTrue("Fetch failed for <" + uniqueId + ">", response.hasResultDocument());
				DBObject dbObject = response.getDocument();
				;
				assertEquals("BSON object is missing field", "val1", dbObject.get("key1"));
				assertEquals("BSON object is missing field", "val2", dbObject.get("key2"));
				
				assertEquals("Expected 3 associated documents", 3, response.getAssociatedDocumentCount());
				assertTrue("Associated Document should be meta only", !response.getAssociatedDocument(0).hasDocument());
				assertTrue("Associated Document should be meta only", !response.getAssociatedDocument(1).hasDocument());
				assertTrue("Associated Document should be meta only", !response.getAssociatedDocument(2).hasDocument());
			}
			{
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				assertTrue("Fetch failed for <" + uniqueId + ">", response.hasResultDocument());
				DBObject dbObject = response.getDocument();
				assertEquals("BSON object is missing field", "val1", dbObject.get("key1"));
				// assertEquals(dbObject.get("key2"), "val2", "BSON object is missing field");
				
				assertEquals("Expected 3 associated documents", 3, response.getAssociatedDocumentCount());
				assertTrue("Associated document does not exist", response.getAssociatedDocument(0).hasDocument());
				assertTrue("Associated document does not exist", response.getAssociatedDocument(1).hasDocument());
				assertTrue("Associated document does not exist", response.getAssociatedDocument(2).hasDocument());
				
			}
			
			{
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				lumongoWorkPool.fetchLargeAssociated(new FetchLargeAssociated(uniqueId, MY_TEST_INDEX, "myfile2", os));
				String text = os.toString("UTF-8");
				assertEquals("Associated document content does not match expected", "Some Other Text", text);
			}
		}
	}
	
	@Test
	public void test07Delete() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		{
			String uniqueIdToDelete = "someUniqueId-" + 4;
			
			QueryResult qr = null;
			FetchResult fr = null;
			
			fr = lumongoWorkPool.fetch(new FetchDocument(uniqueIdToDelete, MY_TEST_INDEX));
			assertTrue("Document is missing raw document before delete", fr.hasResultDocument());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "uid" + ":" + uniqueIdToDelete, 10));
			assertEquals("Total hits is not 1 before delete", 1, qr.getTotalHits());
			
			lumongoWorkPool.delete(new DeleteFull(uniqueIdToDelete, MY_TEST_INDEX));
			
			fr = lumongoWorkPool.fetch(new FetchDocument(uniqueIdToDelete, MY_TEST_INDEX));
			assertTrue("Document has raw document after delete", !fr.hasResultDocument());
			
			qr = lumongoWorkPool.query(new Query(MY_TEST_INDEX, "uid" + ":" + uniqueIdToDelete, 10));
			assertEquals("Total hits is not 0 after delete", 0, qr.getTotalHits());
		}
		
		{
			String uniqueId = "id3333";
			String fileName = "myfile2";
			{
				
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				assertEquals("Expecting 3 associated documents", 3, response.getAssociatedDocumentCount());
			}
			
			{
				lumongoWorkPool.delete(new DeleteAssociated(uniqueId, MY_TEST_INDEX, fileName));
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				assertEquals("Expecting 2 associated document", 2, response.getAssociatedDocumentCount());
			}
			
			{
				lumongoWorkPool.delete(new DeleteAllAssociated(uniqueId, MY_TEST_INDEX));
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				assertEquals("Expecting 0 associated documents", 0, response.getAssociatedDocumentCount());
				assertTrue("Expecting raw document", response.hasResultDocument());
				
			}
			
			{
				lumongoWorkPool.delete(new DeleteFull(uniqueId, MY_TEST_INDEX));
				FetchResult response = lumongoWorkPool.fetch(new FetchDocumentAndAssociated(uniqueId, MY_TEST_INDEX));
				assertEquals("Expecting 0 associated document", 0, response.getAssociatedDocumentCount());
				assertTrue("Expecting no raw document", !response.hasResultDocument());
				
			}
			
		}
	}
	
	@Test
	public void test08Api() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		{
			DBObject object = new BasicDBObject();
			object.put("issn", "4444-1111");
			object.put("title", "A really special title to search");
			
			String uniqueId = "myid123";
			Store s = new Store(uniqueId, MY_TEST_INDEX);
			
			s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(object));
			
			lumongoWorkPool.store(s);
		}
		{
			int numberOfResults = 10;
			String normalLuceneQuery = "issn:1234-1234 AND title:special";
			
			Query q = new Query(MY_TEST_INDEX, normalLuceneQuery, numberOfResults);
			QueryResult qr = lumongoWorkPool.query(q);
			@SuppressWarnings("unused")
			long totalHits = qr.getTotalHits();
			
			for (ScoredResult sr : qr.getResults()) {
				@SuppressWarnings("unused")
				String uniqueId = sr.getUniqueId();
				@SuppressWarnings("unused")
				float score = sr.getScore();
				//System.out.println("Matching document <" + uniqueId + "> with score <" + score + "> and timestamp <" + sr.getTimestamp() + ">");
			}
			
			//System.out.println("Query <" + normalLuceneQuery + "> found <" + totalHits + "> total hits.  Fetched <" + qr.getResults().size() + "> documents.");
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
				//System.out.println("Document: " + fr.getDocumentAsUtf8());
			}
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest(30, "issn");
			
			QueryResult qr = lumongoWorkPool.query(q);
			
			for (@SuppressWarnings("unused")
			FacetCount fc : qr.getFacetCounts("issn")) {
				//System.out.println("Facet <" + fc.getFacet() + "> has <" + fc.getCount() + "> items");
			}
		}
		
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234");
			
			@SuppressWarnings("unused")
			QueryResult qr = lumongoWorkPool.query(q);
			
		}
	}
	
	@Test
	public void test09DeleteIndex() throws Exception {
		LumongoWorkPool lumongoWorkPool = getLumongoWorkPool();

		GetIndexesResult gir = null;
		
		gir = lumongoWorkPool.getIndexes();
		assertEquals("Expected two indexes", 2, gir.getIndexCount());
		lumongoWorkPool.deleteIndex(new DeleteIndex(MY_TEST_INDEX));
		gir = lumongoWorkPool.getIndexes();
		assertEquals("Expected one indexes", 1, gir.getIndexCount());
	}
	
	@AfterClass
	public void test10Shutdown() throws Exception {
		stopSuite();
	}
}
