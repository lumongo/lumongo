package org.lumongo.test.cluster;

import org.lumongo.client.command.CreateIndex;
import org.lumongo.client.command.DeleteAllAssociated;
import org.lumongo.client.command.DeleteAssociated;
import org.lumongo.client.command.DeleteFull;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.FetchDocumentAndAssociated;
import org.lumongo.client.command.GetIndexes;
import org.lumongo.client.command.IndexConfig;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.doc.AssociatedBuilder;
import org.lumongo.doc.IndexedDocBuilder;
import org.lumongo.util.BsonHelper;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class SingleNodeTest {
	private static final String MY_TEST_INDEX = "myTestIndex";

	private static final String FACET_TEST_INDEX = "facetTestIndex";

	private static Logger log = Logger.getLogger(SingleNodeTest.class);

	private LumongoWorkPool lumongoWorkPool;



	@Test(groups = { "init" })
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

		lumongoWorkPool.execute(new CreateIndex(MY_TEST_INDEX, 16, "uid", indexConfig));
		lumongoWorkPool.execute(new CreateIndex(FACET_TEST_INDEX, 16, "uid", indexConfig).setFaceted(true));
	}

	@Test(groups = { "facet" }, dependsOnGroups = { "init" })
	public void facetTest() throws Exception {
		final int COUNT_PER_ISSN = 100;
		final String uniqueIdPrefix = "myId-";
		final String[] issns = new String[] { "1234-1234", "3333-1234", "1234-5555", "1234-4444", "2222-2222" };
		int id = 0;
		{
			for (String issn : issns) {
				for (int i = 0; i < COUNT_PER_ISSN; i++) {
					id++;
					String uniqueId = uniqueIdPrefix + id;

					IndexedDocBuilder docBuilder = new IndexedDocBuilder(FACET_TEST_INDEX);
					docBuilder.addField("issn", issn);
					docBuilder.addField("title", "Facet Userguide");
					docBuilder.addFacet("issn", issn);
					LMDoc indexedDoc = docBuilder.getIndexedDoc();

					boolean compressed = (i % 2 == 0);

					Store s = new Store(uniqueId).setResultDocument("<sampleXML>" + i + "</sampleXML>", compressed).addIndexedDocument(indexedDoc);

					lumongoWorkPool.execute(s);
				}
			}
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest("issn", 30);
			QueryResult qr = lumongoWorkPool.execute(q);

			Assert.assertEquals(qr.getTotalHits(), COUNT_PER_ISSN * issns.length, "Total record count not " + COUNT_PER_ISSN * issns.length);

			Assert.assertEquals(qr.getFacetCountCount(), issns.length, "Total facets not " + issns.length);
			for (FacetCount fc : qr.getFacetCounts()) {
				Assert.assertEquals(fc.getCount(), COUNT_PER_ISSN, "Count for facet <" + fc.getFacet() + "> not <" + COUNT_PER_ISSN + ">");
			}

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn/1234-1234");

			QueryResult qr = lumongoWorkPool.execute(q);

			Assert.assertEquals(qr.getTotalHits(), COUNT_PER_ISSN, "Total record count after drill down not " + COUNT_PER_ISSN);
		}
	}

	@Test(groups = { "first" }, dependsOnGroups = { "init" })
	public void bulkTest() throws Exception {

		final int DOCUMENTS_LOADED = 5;
		final String uniqueIdPrefix = "someUniqueId-";
		{
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;


				IndexedDocBuilder docBuilder = new IndexedDocBuilder(MY_TEST_INDEX);
				docBuilder.addField("issn", "1333-1333");
				docBuilder.addField("title", "Search and Storage");
				LMDoc indexedDoc = docBuilder.getIndexedDoc();


				boolean compressed = (i % 2 == 0);

				Store s = new Store(uniqueId).setResultDocument("<sampleXML>random xml</sampleXML>", compressed);
				s.addIndexedDocument(indexedDoc);
				lumongoWorkPool.execute(s);
			}

			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;

				IndexedDocBuilder docBuilder = new IndexedDocBuilder(MY_TEST_INDEX);
				docBuilder.addField("issn", "1234-1234");
				docBuilder.addField("title", "Distributed Search and Storage System");
				docBuilder.addField("an", i);
				LMDoc indexedDoc = docBuilder.getIndexedDoc();

				boolean compressed = (i % 2 == 0);

				Store s = new Store(uniqueId).setResultDocument("<sampleXML>" + i + "</sampleXML>", compressed);
				s.addIndexedDocument(indexedDoc);
				lumongoWorkPool.execute(s);
			}
		}
		{
			QueryResult qr = null;

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "an:3", 10));
			Assert.assertEquals(qr.getTotalHits(), 1, "Total hits is not 1");

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "an:[1 TO 3]", 10));
			Assert.assertEquals(qr.getTotalHits(), 3, "Total hits is not 3");

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "an:{1 TO 3}", 10));
			Assert.assertEquals(qr.getTotalHits(), 1, "Total hits is not 1");

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "title:distributed", 300));
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "title:distributed", 100));
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "distributed", 20));
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "issn:1234-1234", 20));
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "title:cluster", 10));
			Assert.assertEquals(qr.getTotalHits(), 0, "Total hits is not 0");

		}

		{
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;

				FetchResult response = lumongoWorkPool.execute(new FetchDocument(uniqueId));
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				String recordText = response.getDocumentAsUtf8();
				System.out.println("\n:" + recordText + ":\n");
				Assert.assertTrue(recordText.equals("<sampleXML>" + i + "</sampleXML>"), "Document contents is invalid for <" + uniqueId + ">");
			}
		}
	}

	@Test(groups = { "first" }, dependsOnGroups = { "init" })
	public void bsonTest() throws Exception {

		String uniqueId = "bsonTestObjectId";
		{
			IndexedDocBuilder docBuilder = new IndexedDocBuilder(MY_TEST_INDEX);
			docBuilder.addField("issn", "4321-4321");
			docBuilder.addField("title", "Magic Java Beans");
			docBuilder.addField("eissn", "3333-3333");
			LMDoc indexedDoc = docBuilder.getIndexedDoc();

			DBObject dbObject = new BasicDBObject();
			dbObject.put("someKey", "someValue");
			dbObject.put("other key", "other value");

			Store s = new Store(uniqueId).setResultDocument(dbObject).addIndexedDocument(indexedDoc);
			lumongoWorkPool.execute(s);
		}

		{
			FetchResult response = lumongoWorkPool.execute(new FetchDocument(uniqueId));
			Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
			DBObject dbObject = response.getDocumentAsBson();
			Assert.assertEquals(dbObject.get("someKey"), "someValue", "BSON object is missing field");
			Assert.assertEquals(dbObject.get("other key"), "other value", "BSON object is missing field");
		}

	}

	@Test(groups = { "first" }, dependsOnGroups = { "init" })
	public void associatedTest() throws Exception {

		String uniqueId = "id3333";
		{
			{
				IndexedDocBuilder docBuilder = new IndexedDocBuilder(MY_TEST_INDEX);
				docBuilder.addField("issn", "6666-6666");
				docBuilder.addField("title", "More Magic Java Beans");
				docBuilder.addField("eissn", 2222 - 1111);
				LMDoc indexedDoc = docBuilder.getIndexedDoc();

				DBObject dbObject = new BasicDBObject();
				dbObject.put("key1", "val1");
				dbObject.put("key2", "val2");

				AssociatedBuilder associatedBuilder = new AssociatedBuilder(uniqueId, "myfile");
				associatedBuilder.setCompressed(true);
				associatedBuilder.setDocument("Some Text");
				associatedBuilder.addMetaData("mydata", "myvalue");
				associatedBuilder.addMetaData("sometypeinfo", "text file");
				AssociatedDocument ad = associatedBuilder.getAssociatedDocument();

				Store s = new Store(uniqueId);
				s.addIndexedDocument(indexedDoc);
				s.setResultDocument(dbObject);
				s.addAssociatedDocument(ad);

				lumongoWorkPool.execute(s);

			}
			{

				AssociatedBuilder associatedBuilder = new AssociatedBuilder(uniqueId, "myfile2");
				associatedBuilder.setCompressed(false);
				associatedBuilder.setDocument("Some Other Text");
				associatedBuilder.addMetaData("mydata", "myvalue 2");
				associatedBuilder.addMetaData("sometypeinfo", "text file");
				AssociatedDocument ad = associatedBuilder.getAssociatedDocument();

				Store s = new Store(uniqueId);
				s.addAssociatedDocument(ad);
				lumongoWorkPool.execute(s);
			}
			{
				AssociatedBuilder associatedBuilder = new AssociatedBuilder(uniqueId, "filef");
				associatedBuilder.setCompressed(true);
				associatedBuilder.setDocument("Some Other Text");
				associatedBuilder.addMetaData("stuff", "mystuff");
				AssociatedDocument ad = associatedBuilder.getAssociatedDocument();

				Store s = new Store(uniqueId);
				s.addAssociatedDocument(ad);
				lumongoWorkPool.execute(s);
			}
		}
		{
			{
				FetchResult response = lumongoWorkPool.execute(new FetchDocumentAndAssociated(uniqueId, true));

				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				ResultDocument rd = response.getResultDocument();
				DBObject dbObject = BsonHelper.dbObjectFromResultDocument(rd);
				Assert.assertEquals(dbObject.get("key1"), "val1", "BSON object is missing field");
				Assert.assertEquals(dbObject.get("key2"), "val2", "BSON object is missing field");

				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expected 3 associated documents");
				Assert.assertTrue(!response.getAssociatedDocument(0).hasDocument(), "Associated Document should be meta only");
				Assert.assertTrue(!response.getAssociatedDocument(1).hasDocument(), "Associated Document should be meta only");
				Assert.assertTrue(!response.getAssociatedDocument(2).hasDocument(), "Associated Document should be meta only");
			}
			{
				FetchResult response = lumongoWorkPool.execute(new FetchDocumentAndAssociated(uniqueId));
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				ResultDocument rd = response.getResultDocument();
				DBObject dbObject = BsonHelper.dbObjectFromResultDocument(rd);
				Assert.assertEquals(dbObject.get("key1"), "val1", "BSON object is missing field");
				// Assert.assertEquals(dbObject.get("key2"), "val2", "BSON object is missing field");

				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expected 3 associated documents");
				Assert.assertTrue(response.getAssociatedDocument(0).hasDocument(), "Associated document does not exist");
				Assert.assertTrue(response.getAssociatedDocument(1).hasDocument(), "Associated document does not exist");
				Assert.assertTrue(response.getAssociatedDocument(2).hasDocument(), "Associated document does not exist");

			}
		}
	}

	@Test(groups = { "next" }, dependsOnGroups = { "first" })
	public void deleteTest() throws Exception {
		{
			String uniqueIdToDelete = "someUniqueId-" + 4;

			QueryResult qr = null;
			FetchResult fr = null;

			fr = lumongoWorkPool.execute(new FetchDocument(uniqueIdToDelete));
			Assert.assertTrue(fr.hasResultDocument(), "Document is missing raw document before delete");

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "uid" + ":" + uniqueIdToDelete, 10));
			Assert.assertEquals(qr.getTotalHits(), 1, "Total hits is not 1 before delete");

			lumongoWorkPool.execute(new DeleteFull(uniqueIdToDelete));

			fr = lumongoWorkPool.execute(new FetchDocument(uniqueIdToDelete));
			Assert.assertTrue(!fr.hasResultDocument(), "Document has raw document after delete");

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "uid" + ":" + uniqueIdToDelete, 10));
			Assert.assertEquals(qr.getTotalHits(), 0, "Total hits is not 0 after delete");
		}

		{
			String uniqueId = "id3333";
			String fileName = "myfile2";
			{

				FetchResult response = lumongoWorkPool.execute(new FetchDocumentAndAssociated(uniqueId));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expecting 3 associated documents");
			}

			{
				lumongoWorkPool.execute(new DeleteAssociated(uniqueId, fileName));
				FetchResult response = lumongoWorkPool.execute(new FetchDocumentAndAssociated(uniqueId));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 2, "Expecting 2 associated document");
			}

			{
				lumongoWorkPool.execute(new DeleteAllAssociated(uniqueId));
				FetchResult response = lumongoWorkPool.execute(new FetchDocumentAndAssociated(uniqueId));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 0, "Expecting 0 associated documents");
				Assert.assertTrue(response.hasResultDocument(), "Expecting raw document");

			}

			{
				lumongoWorkPool.execute(new DeleteFull(uniqueId));
				FetchResult response = lumongoWorkPool.execute(new FetchDocumentAndAssociated(uniqueId));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 0, "Expecting 0 associated document");
				Assert.assertTrue(!response.hasResultDocument(), "Expecting no raw document");

			}

		}
	}

	@Test(groups = { "last" }, dependsOnGroups = { "next" })
	public void apiUsage() throws Exception {
		{
			IndexedDocBuilder docBuilder = new IndexedDocBuilder(MY_TEST_INDEX);
			docBuilder.addField("issn", "4444-1111");
			docBuilder.addField("title", "A really special title to search");
			LMDoc indexedDoc = docBuilder.getIndexedDoc();

			String uniqueId = "myid123";
			Store s = new Store(uniqueId);
			s.addIndexedDocument(indexedDoc);
			s.setResultDocument("<sampleXML></sampleXML>", true);

			lumongoWorkPool.execute(s);
		}
		{
			int numberOfResults = 10;
			String normalLuceneQuery = "issn:1234-1234 AND title:special";

			Query q = new Query(MY_TEST_INDEX, normalLuceneQuery, numberOfResults);
			QueryResult qr = lumongoWorkPool.execute(q);
			long totalHits = qr.getTotalHits();

			for (ScoredResult sr : qr.getResults()) {
				String uniqueId = sr.getUniqueId();
				float score = sr.getScore();
				System.out.println("Matching document <" + uniqueId + "> with score <" + score + ">");
			}

			System.out.println("Query <" + normalLuceneQuery + "> found <" + totalHits + "> total hits.  Fetched <" + qr.getResults().size()
					+ "> documents.");
		}

		{
			int numberOfResults = 10;
			String normalLuceneQuery = "title:special";

			Query q = new Query(MY_TEST_INDEX, normalLuceneQuery, numberOfResults);

			QueryResult first = lumongoWorkPool.execute(q);

			@SuppressWarnings("unused")
			QueryResult next = lumongoWorkPool.execute(q.setLastResult(first));

		}

		{
			String uniqueIdToDelete = "someId";
			lumongoWorkPool.execute(new DeleteFull(uniqueIdToDelete));
		}

		{
			String uniqueId = "someId";
			FetchResult fr = lumongoWorkPool.execute(new FetchDocument(uniqueId));
			if (!fr.hasResultDocument()) {
				System.out.println("Document: " + fr.getDocumentAsUtf8());
			}
		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addCountRequest("issn", 30);

			QueryResult qr = lumongoWorkPool.execute(q);

			for (FacetCount fc : qr.getFacetCounts()) {
				System.out.println("Facet <" + fc.getFacet() + "> has <" + fc.getCount() + "> items");
			}
		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn/1234-1234");

			@SuppressWarnings("unused")
			QueryResult qr = lumongoWorkPool.execute(q);


		}
	}

	@Test(groups = { "drop" }, dependsOnGroups = { "last", "facet" })
	public void dropIndex() throws Exception {
		GetIndexesResult gir = null;

		gir = lumongoWorkPool.execute(new GetIndexes());
		Assert.assertEquals(gir.getIndexCount(), 2, "Expected two indexes");
		lumongoWorkPool.execute(new DeleteIndex(MY_TEST_INDEX));
		gir = lumongoWorkPool.execute(new GetIndexes());
		Assert.assertEquals(gir.getIndexCount(), 1, "Expected one indexes");
	}
}
