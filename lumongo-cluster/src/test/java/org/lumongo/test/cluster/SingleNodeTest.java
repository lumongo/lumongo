package org.lumongo.test.cluster;

import java.io.IOException;
import java.util.HashMap;

import org.lumongo.LumongoConstants;
import org.lumongo.client.command.CreateIndex;
import org.lumongo.client.command.Delete;
import org.lumongo.client.command.DeleteAllAssociated;
import org.lumongo.client.command.DeleteAssociated;
import org.lumongo.client.command.DeleteIndex;
import org.lumongo.client.command.FetchDocument;
import org.lumongo.client.command.FetchDocumentAndAssociated;
import org.lumongo.client.command.GetIndexes;
import org.lumongo.client.command.Query;
import org.lumongo.client.command.Store;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.client.result.FetchResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.server.LuceneNode;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.storage.rawfiles.MongoDocumentStorage;
import org.lumongo.util.BsonHelper;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LogUtil;
import org.lumongo.util.ServerNameHelper;
import org.lumongo.util.TestHelper;
import org.lumongo.util.properties.FakePropertiesReader;
import org.lumongo.util.properties.PropertiesReader.PropertyException;
import org.testng.Assert;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class SingleNodeTest {
	private static final String MY_TEST_INDEX = "myTestIndex";

	private static final String FACET_TEST_INDEX = "facetTestIndex";

	private static Logger log = Logger.getLogger(SingleNodeTest.class);

	private LumongoWorkPool lumongoWorkPool;

	protected LuceneNode luceneNode;

	@BeforeSuite
	public static void cleanDatabaseAndInit() throws Exception {

		Thread.currentThread().setName("Test");

		LogUtil.loadLogConfig();

		log.info("Cleaning existing test dbs and initing");

		Mongo mongo = TestHelper.getMongo();
		mongo.getDB(TestHelper.TEST_DATABASE_NAME).dropDatabase();
		mongo.getDB(TestHelper.TEST_DATABASE_NAME + MongoDocumentStorage.STORAGE_DB_SUFFIX).dropDatabase();
	}

	@BeforeGroups(groups = { "init" })
	public void startServer() throws Exception {

		log.info("Starting server for single node test");

		MongoConfig mongoConfig = getTestMongoConfig();
		LocalNodeConfig localNodeConfig = getTestLocalNodeConfig();

		ClusterConfig clusterConfig = getTestClusterConfig();
		ClusterHelper.saveClusterConfig(mongoConfig, clusterConfig);

		String localServer = ServerNameHelper.getLocalServer();

		ClusterHelper.registerNode(mongoConfig, localNodeConfig, localServer);

		int instance = localNodeConfig.getHazelcastPort();

		luceneNode = new LuceneNode(mongoConfig, localServer, instance);

		luceneNode.start();

		startClient();

	}

	public ClusterConfig getTestClusterConfig() throws PropertyException {
		HashMap<String, String> settings = new HashMap<String, String>();

		settings.put(ClusterConfig.SHARDED, "false");
		settings.put(ClusterConfig.INDEX_BLOCK_SIZE, "131072");
		settings.put(ClusterConfig.MAX_INDEX_BLOCKS, "10000");
		settings.put(ClusterConfig.MAX_INTERNAL_CLIENT_CONNECTIONS, "16");
		settings.put(ClusterConfig.INTERNAL_WORKERS, "16");
		settings.put(ClusterConfig.EXTERNAL_WORKERS, "16");
		settings.put(ClusterConfig.INTERNAL_SHUTDOWN_TIMEOUT, "10");
		settings.put(ClusterConfig.EXTERNAL_SHUTDOWN_TIMEOUT, "10");

		ClusterConfig clusterConfig = new ClusterConfig(new FakePropertiesReader("test", settings));
		return clusterConfig;
	}

	public LocalNodeConfig getTestLocalNodeConfig() throws PropertyException {
		HashMap<String, String> settings = new HashMap<String, String>();

		settings.put(LocalNodeConfig.HAZELCAST_PORT, LumongoConstants.DEFAULT_HAZELCAST_PORT + "");
		settings.put(LocalNodeConfig.INTERNAL_SERVICE_PORT, LumongoConstants.DEFAULT_INTERNAL_SERVICE_PORT + "");
		settings.put(LocalNodeConfig.EXTERNAL_SERVICE_PORT, LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT + "");
		settings.put(LocalNodeConfig.REST_PORT, LumongoConstants.DEFAULT_REST_SERVICE_PORT + "");
		LocalNodeConfig localNodeConfig = new LocalNodeConfig(new FakePropertiesReader("test", settings));
		return localNodeConfig;
	}

	public MongoConfig getTestMongoConfig() throws PropertyException {
		HashMap<String, String> settings = new HashMap<String, String>();

		settings.put(MongoConfig.DATABASE_NAME, TestHelper.TEST_DATABASE_NAME);
		settings.put(MongoConfig.MONGO_HOST, TestHelper.getMongoServer());
		settings.put(MongoConfig.MONGO_PORT, String.valueOf(TestHelper.getMongoPort()));

		MongoConfig mongoConfig = new MongoConfig(new FakePropertiesReader("test", settings));
		return mongoConfig;
	}

	@AfterGroups(groups = { "drop" })
	public void stopServer() throws Exception {
		log.info("Stopping server for single node test");
		stopClient();
		luceneNode.shutdown();
	}

	public void startClient() throws IOException {
		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
		lumongoPoolConfig.addMember("localhost");
		lumongoWorkPool = new LumongoWorkPool(new LumongoPool(lumongoPoolConfig));
	}

	public void stopClient() throws Exception {
		lumongoWorkPool.shutdown();
	}

	@Test(groups = { "init" })
	public void createIndexTest() throws Exception {

		IndexSettings.Builder indexSettingsBuilder = IndexSettings.newBuilder();
		indexSettingsBuilder.setDefaultSearchField("title");
		indexSettingsBuilder.setDefaultAnalyzer(LMAnalyzer.KEYWORD);
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("title").setAnalyzer(LMAnalyzer.STANDARD));
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("issn").setAnalyzer(LMAnalyzer.LC_KEYWORD));
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("uid").setAnalyzer(LMAnalyzer.LC_KEYWORD));
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("an").setAnalyzer(LMAnalyzer.NUMERIC_INT));
		indexSettingsBuilder.setSegmentTolerance(0.05);

		lumongoWorkPool.execute(new CreateIndex(MY_TEST_INDEX, 16, "uid", indexSettingsBuilder.build()));

		indexSettingsBuilder = IndexSettings.newBuilder();
		indexSettingsBuilder.setDefaultSearchField("title");
		indexSettingsBuilder.setDefaultAnalyzer(LMAnalyzer.KEYWORD);
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("title").setAnalyzer(LMAnalyzer.STANDARD));
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("issn").setAnalyzer(LMAnalyzer.LC_KEYWORD));
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("uid").setAnalyzer(LMAnalyzer.LC_KEYWORD));
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("an").setAnalyzer(LMAnalyzer.NUMERIC_INT));
		indexSettingsBuilder.setSegmentTolerance(0.05);

		lumongoWorkPool.execute(new CreateIndex(FACET_TEST_INDEX, 16, "uid", indexSettingsBuilder.build()).setFaceted(true));
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
					LMDoc.Builder indexedDocBuilder = LMDoc.newBuilder();
					indexedDocBuilder.setIndexName(FACET_TEST_INDEX);
					indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue(issn).build());
					indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("Facet Userguide").build());
					indexedDocBuilder.addFacet("issn" + LumongoConstants.FACET_DELIMITER + issn);
					LMDoc indexedDoc = indexedDocBuilder.build();

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
				LMDoc.Builder indexedDocBuilder = LMDoc.newBuilder();
				indexedDocBuilder.setIndexName(MY_TEST_INDEX);
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("1333-1333").build());
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("Search and Storage").build());
				LMDoc indexedDoc = indexedDocBuilder.build();

				boolean compressed = (i % 2 == 0);

				Store s = new Store(uniqueId).setResultDocument("<sampleXML>random xml</sampleXML>", compressed);
				s.addIndexedDocument(indexedDoc);
				lumongoWorkPool.execute(s);
			}

			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				LMDoc.Builder indexedDocBuilder = LMDoc.newBuilder();
				indexedDocBuilder.setIndexName(MY_TEST_INDEX);
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("1234-1234").build());
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("Distributed Search and Storage System").build());
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("an").addIntValue(i).build());
				LMDoc indexedDoc = indexedDocBuilder.build();

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
			LMDoc.Builder indexedDocBuilder = LMDoc.newBuilder();
			indexedDocBuilder.setIndexName(MY_TEST_INDEX);
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("4321-4321").build());
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("Magic Java Beans").build());
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("eissn").addFieldValue("3333-3333").build());
			LMDoc indexedDoc = indexedDocBuilder.build();

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
				LMDoc.Builder indexedDocBuilder = LMDoc.newBuilder();
				indexedDocBuilder.setIndexName(MY_TEST_INDEX);
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("6666-6666").build());
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("More Magic Java Beans").build());
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("eissn").addFieldValue("2222-1111").build());
				LMDoc indexedDoc = indexedDocBuilder.build();

				DBObject dbObject = new BasicDBObject();
				dbObject.put("key1", "val1");
				dbObject.put("key2", "val2");

				AssociatedDocument.Builder adBuilder = AssociatedDocument.newBuilder();
				adBuilder.setCompressed(true);
				adBuilder.setDocument(ByteString.copyFromUtf8("Some Text"));
				adBuilder.addMetadata(Metadata.newBuilder().setKey("mydata").setValue("myvalue"));
				adBuilder.addMetadata(Metadata.newBuilder().setKey("sometypeinfo").setValue("text file"));
				adBuilder.setFilename("myfile");
				adBuilder.setDocumentUniqueId(uniqueId);
				AssociatedDocument ad = adBuilder.build();

				Store s = new Store(uniqueId);
				s.addIndexedDocument(indexedDoc);
				s.setResultDocument(dbObject);
				s.addAssociatedDocument(ad);

				lumongoWorkPool.execute(s);

			}
			{
				AssociatedDocument.Builder adBuilder = AssociatedDocument.newBuilder();
				adBuilder.setCompressed(false);
				adBuilder.setDocument(ByteString.copyFromUtf8("Some Other Text"));
				adBuilder.addMetadata(Metadata.newBuilder().setKey("mydata").setValue("myvalue 2"));
				adBuilder.addMetadata(Metadata.newBuilder().setKey("sometypeinfo").setValue("text file"));
				adBuilder.setFilename("myfile2");
				adBuilder.setDocumentUniqueId(uniqueId);
				AssociatedDocument ad = adBuilder.build();

				Store s = new Store(uniqueId);
				s.addAssociatedDocument(ad);
				lumongoWorkPool.execute(s);
			}
			{
				AssociatedDocument.Builder adBuilder = AssociatedDocument.newBuilder();
				adBuilder.setCompressed(true);
				adBuilder.setDocument(ByteString.copyFromUtf8("Some Other Text"));
				adBuilder.addMetadata(Metadata.newBuilder().setKey("stuff").setValue("mystuff"));
				adBuilder.setFilename("filef");
				adBuilder.setDocumentUniqueId(uniqueId);
				AssociatedDocument ad = adBuilder.build();
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
				Assert.assertTrue(!response.getAssociatedDocuments().get(0).hasDocument(), "Associated Document should be meta only");
				Assert.assertTrue(!response.getAssociatedDocuments().get(1).hasDocument(), "Associated Document should be meta only");
				Assert.assertTrue(!response.getAssociatedDocuments().get(2).hasDocument(), "Associated Document should be meta only");
			}
			{
				FetchResult response = lumongoWorkPool.execute(new FetchDocumentAndAssociated(uniqueId));
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				ResultDocument rd = response.getResultDocument();
				DBObject dbObject = BsonHelper.dbObjectFromResultDocument(rd);
				Assert.assertEquals(dbObject.get("key1"), "val1", "BSON object is missing field");
				Assert.assertEquals(dbObject.get("key2"), "val2", "BSON object is missing field");

				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expected 3 associated documents");
				Assert.assertTrue(response.getAssociatedDocuments().get(0).hasDocument(), "Associated document does not exist");
				Assert.assertTrue(response.getAssociatedDocuments().get(1).hasDocument(), "Associated document does not exist");
				Assert.assertTrue(response.getAssociatedDocuments().get(2).hasDocument(), "Associated document does not exist");

			}
		}
	}

	@Test(groups = { "next" }, dependsOnGroups = { "init" })
	public void deleteTest() throws Exception {
		{
			String uniqueIdToDelete = "someUniqueId-" + 4;

			QueryResult qr = null;
			FetchResult fr = null;

			fr = lumongoWorkPool.execute(new FetchDocument(uniqueIdToDelete));
			Assert.assertTrue(fr.hasResultDocument(), "Document is missing raw document before delete");

			qr = lumongoWorkPool.execute(new Query(MY_TEST_INDEX, "uid" + ":" + uniqueIdToDelete, 10));
			Assert.assertEquals(qr.getTotalHits(), 1, "Total hits is not 1 before delete");

			lumongoWorkPool.execute(new Delete(uniqueIdToDelete));

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
				lumongoWorkPool.execute(new Delete(uniqueId));
				FetchResult response = lumongoWorkPool.execute(new FetchDocumentAndAssociated(uniqueId));
				Assert.assertEquals(response.getAssociatedDocumentCount(), 0, "Expecting 0 associated document");
				Assert.assertTrue(!response.hasResultDocument(), "Expecting no raw document");

			}

		}
	}

	@Test(groups = { "last" }, dependsOnGroups = { "next" })
	public void apiUsage() throws Exception {
		{
			LMDoc.Builder indexedDocBuilder = LMDoc.newBuilder();
			indexedDocBuilder.setIndexName(MY_TEST_INDEX);
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("4444-1111").build());
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("A really special title to search").build());
			LMDoc indexedDoc = indexedDocBuilder.build();

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
			lumongoWorkPool.execute(new Delete(uniqueIdToDelete));
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
