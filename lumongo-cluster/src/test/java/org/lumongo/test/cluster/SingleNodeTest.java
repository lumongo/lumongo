package org.lumongo.test.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.lumongo.LumongoConstants;
import org.lumongo.client.LumongoClient;
import org.lumongo.client.config.LumongoClientConfig;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.server.LuceneNode;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.storage.rawfiles.MongoDocumentStorage;
import org.lumongo.util.BSONHelper;
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
	private static Logger log = Logger.getLogger(SingleNodeTest.class);
	
	private LumongoClient lumongoClient;
	
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
		settings.put(ClusterConfig.MAX_DIRTY_INDEX_BLOCKS, "12500");
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
	
	@AfterGroups(groups = { "last" })
	public void stopServer() throws Exception {
		
		log.info("Stopping server for single node test");
		stopClient();
		luceneNode.shutdown();
	}
	
	public void startClient() throws IOException {
		LumongoClientConfig lumongoClientConfig = new LumongoClientConfig();
		lumongoClientConfig.addMember("localhost");
		lumongoClient = new LumongoClient(lumongoClientConfig);
	}
	
	public void stopClient() throws IOException {
		lumongoClient.close();
	}
	
	@Test(groups = { "init" })
	public void createIndexTest() throws Exception {
		
		IndexSettings.Builder indexSettingsBuilder = IndexSettings.newBuilder();
		indexSettingsBuilder.setDefaultSearchField("title");
		indexSettingsBuilder.setDefaultAnalyzer(LMAnalyzer.KEYWORD);
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("title").setAnalyzer(LMAnalyzer.STANDARD));
		indexSettingsBuilder.addFieldConfig(FieldConfig.newBuilder().setFieldName("issn").setAnalyzer(LMAnalyzer.LC_KEYWORD));
		indexSettingsBuilder.setSegmentTolerance(0.1);
		
		lumongoClient.createIndex("myTestIndex", 16, "uid", indexSettingsBuilder.build());
		
	}
	
	@Test(groups = { "first" }, dependsOnGroups = { "init" })
	public void bulkTest() throws Exception {
		
		final int DOCUMENTS_LOADED = 100;
		final String uniqueIdPrefix = "someUniqueId";
		{
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				LMDoc.Builder indexedDocBuilder = LMDoc.newBuilder();
				indexedDocBuilder.setIndexName("myTestIndex");
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("1234-1234").build());
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("Distributed Search and Storage System").build());
				
				ByteString byteString = ByteString.copyFromUtf8("<sampleXML>" + i + "</sampleXML>");
				
				boolean compressed = (i % 2 == 0);
				
				ResultDocument rd = ResultDocument.newBuilder().setType(ResultDocument.Type.TEXT).setDocument(byteString).setUniqueId(uniqueId)
						.setCompressed(compressed).build();
				lumongoClient.storeDocument(rd, indexedDocBuilder.build());
			}
		}
		{
			QueryResponse qr = null;
			
			qr = lumongoClient.query("title:distributed", 300, "myTestIndex");
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);
			
			qr = lumongoClient.query("title:distributed", 100, "myTestIndex");
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);
			
			qr = lumongoClient.query("distributed", 20, "myTestIndex");
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);
			
			qr = lumongoClient.query("issn:1234-1234", 20, "myTestIndex");
			Assert.assertEquals(qr.getTotalHits(), DOCUMENTS_LOADED, "Total hits is not " + DOCUMENTS_LOADED);
			
			qr = lumongoClient.query("title:cluster", 10, "myTestIndex");
			Assert.assertEquals(qr.getTotalHits(), 0, "Total hits is not 0");
		}
		
		{
			for (int i = 0; i < DOCUMENTS_LOADED; i++) {
				String uniqueId = uniqueIdPrefix + i;
				FetchResponse response = lumongoClient.fetchDocument(uniqueId);
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				ResultDocument rd = response.getResultDocument();
				String recordText = rd.getDocument().toStringUtf8();
				Assert.assertTrue(recordText.equals("<sampleXML>" + i + "</sampleXML>"), "Document contents is invalid for <" + uniqueId + ">");
			}
		}
	}
	
	@Test(groups = { "first" }, dependsOnGroups = { "init" })
	public void bsonTest() throws Exception {
		
		String uniqueId = "bsonTestObjectId";
		{
			LMDoc.Builder indexedDocBuilder = LMDoc.newBuilder();
			indexedDocBuilder.setIndexName("myTestIndex");
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("4321-4321").build());
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("Magic Java Beans").build());
			indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("eissn").addFieldValue("3333-3333").build());
			
			DBObject dbObject = new BasicDBObject();
			dbObject.put("someKey", "someValue");
			dbObject.put("other key", "other value");
			ResultDocument rd = BSONHelper.dbObjectToResultDocument(uniqueId, dbObject);
			
			lumongoClient.storeDocument(rd, indexedDocBuilder.build());
		}
		
		{
			FetchResponse response = lumongoClient.fetchDocument(uniqueId);
			Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
			ResultDocument rd = response.getResultDocument();
			DBObject dbObject = BSONHelper.dbObjectFromResultDocument(rd);
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
				indexedDocBuilder.setIndexName("myTestIndex");
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("6666-6666").build());
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("More Magic Java Beans").build());
				indexedDocBuilder.addIndexedField(LMField.newBuilder().setFieldName("eissn").addFieldValue("2222-1111").build());
				DBObject dbObject = new BasicDBObject();
				dbObject.put("key1", "val1");
				dbObject.put("key2", "val2");
				ResultDocument rd = BSONHelper.dbObjectToResultDocument(uniqueId, dbObject);
				
				AssociatedDocument.Builder adBuilder = AssociatedDocument.newBuilder();
				adBuilder.setCompressed(true);
				adBuilder.setDocument(ByteString.copyFromUtf8("Some Text"));
				adBuilder.addMetadata(Metadata.newBuilder().setKey("mydata").setValue("myvalue"));
				adBuilder.addMetadata(Metadata.newBuilder().setKey("sometypeinfo").setValue("text file"));
				adBuilder.setFilename("myfile");
				adBuilder.setDocumentUniqueId(uniqueId);
				AssociatedDocument ad = adBuilder.build();
				
				List<AssociatedDocument> associatedDocuments = new ArrayList<AssociatedDocument>();
				associatedDocuments.add(ad);
				
				lumongoClient.storeDocumentWithAssociated(rd, indexedDocBuilder.build(), associatedDocuments);
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
				lumongoClient.storeAssociatedDocument(ad);
			}
			{
				AssociatedDocument.Builder adBuilder = AssociatedDocument.newBuilder();
				adBuilder.setCompressed(true);
				adBuilder.setDocument(ByteString.copyFromUtf8("Some Other Text"));
				adBuilder.addMetadata(Metadata.newBuilder().setKey("stuff").setValue("mystuff"));
				adBuilder.setFilename("filef");
				adBuilder.setDocumentUniqueId(uniqueId);
				AssociatedDocument ad = adBuilder.build();
				lumongoClient.storeAssociatedDocument(ad);
			}
		}
		{
			{
				FetchResponse response = lumongoClient.fetchDocumentAndAssociatedMeta(uniqueId);
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				ResultDocument rd = response.getResultDocument();
				DBObject dbObject = BSONHelper.dbObjectFromResultDocument(rd);
				Assert.assertEquals(dbObject.get("key1"), "val1", "BSON object is missing field");
				Assert.assertEquals(dbObject.get("key2"), "val2", "BSON object is missing field");
				
				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expected 3 associated documents");
				Assert.assertTrue(!response.getAssociatedDocument(0).hasDocument(), "Associated Document should be meta only");
				Assert.assertTrue(!response.getAssociatedDocument(1).hasDocument(), "Associated Document should be meta only");
				Assert.assertTrue(!response.getAssociatedDocument(2).hasDocument(), "Associated Document should be meta only");
			}
			{
				FetchResponse response = lumongoClient.fetchDocumentAndAssociated(uniqueId);
				Assert.assertTrue(response.hasResultDocument(), "Fetch failed for <" + uniqueId + ">");
				ResultDocument rd = response.getResultDocument();
				DBObject dbObject = BSONHelper.dbObjectFromResultDocument(rd);
				Assert.assertEquals(dbObject.get("key1"), "val1", "BSON object is missing field");
				Assert.assertEquals(dbObject.get("key2"), "val2", "BSON object is missing field");
				
				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expected 3 associated documents");
				Assert.assertTrue(response.getAssociatedDocument(0).hasDocument(), "Associated document does not exist");
				Assert.assertTrue(response.getAssociatedDocument(1).hasDocument(), "Associated document does not exist");
				Assert.assertTrue(response.getAssociatedDocument(2).hasDocument(), "Associated document does not exist");
				
			}
		}
	}
	
	@Test(groups = { "next" }, dependsOnGroups = { "init" })
	public void deleteTest() throws Exception {
		{
			String uniqueIdToDelete = "someUniqueId" + 4;
			
			QueryResponse qr = null;
			FetchResponse fr = null;
			
			fr = lumongoClient.fetchDocument(uniqueIdToDelete);
			Assert.assertTrue(fr.hasResultDocument(), "Document is missing raw document before delete");
			
			qr = lumongoClient.query("uid" + ":" + uniqueIdToDelete, 10, "myTestIndex");
			Assert.assertEquals(qr.getTotalHits(), 1, "Total hits is not 1 before delete");
			
			lumongoClient.delete(uniqueIdToDelete);
			
			fr = lumongoClient.fetchDocument(uniqueIdToDelete);
			Assert.assertTrue(!fr.hasResultDocument(), "Document has raw document after delete");
			
			qr = lumongoClient.query("uid" + ":" + uniqueIdToDelete, 10, "myTestIndex");
			Assert.assertEquals(qr.getTotalHits(), 0, "Total hits is not 0 after delete");
		}
		
		{
			String uniqueId = "id3333";
			String fileName = "myfile2";
			{
				
				FetchResponse response = lumongoClient.fetchDocumentAndAssociated(uniqueId);
				Assert.assertEquals(response.getAssociatedDocumentCount(), 3, "Expecting 3 associated documents");
			}
			
			{
				lumongoClient.deleteAssociated(uniqueId, fileName);
				FetchResponse response = lumongoClient.fetchDocumentAndAssociated(uniqueId);
				Assert.assertEquals(response.getAssociatedDocumentCount(), 2, "Expecting 2 associated document");
			}
			
			{
				lumongoClient.deleteAllAssociated(uniqueId);
				FetchResponse response = lumongoClient.fetchDocumentAndAssociated(uniqueId);
				Assert.assertEquals(response.getAssociatedDocumentCount(), 0, "Expecting 0 associated documents");
				Assert.assertTrue(response.hasResultDocument(), "Expecting raw document");
				
			}
			
			{
				lumongoClient.delete(uniqueId);
				FetchResponse response = lumongoClient.fetchDocumentAndAssociated(uniqueId);
				Assert.assertEquals(response.getAssociatedDocumentCount(), 0, "Expecting 0 associated document");
				Assert.assertTrue(!response.hasResultDocument(), "Expecting no raw document");
				
			}
			
		}
	}
	
	@Test(groups = { "last" }, dependsOnGroups = { "next" })
	public void apiUsage() throws Exception {
		{
			LMDoc.Builder indexedDoc = LMDoc.newBuilder();
			indexedDoc.setIndexName("myTestIndex");
			indexedDoc.addIndexedField(LMField.newBuilder().setFieldName("issn").addFieldValue("4444-1111").build());
			indexedDoc.addIndexedField(LMField.newBuilder().setFieldName("title").addFieldValue("A really special title to search").build());
			
			ResultDocument.Builder resultDocumentBuilder = ResultDocument.newBuilder();
			resultDocumentBuilder.setType(ResultDocument.Type.TEXT);
			resultDocumentBuilder.setDocument(ByteString.copyFromUtf8("<sampleXML></sampleXML>"));
			resultDocumentBuilder.setUniqueId("myid123");
			resultDocumentBuilder.setCompressed(true);
			lumongoClient.storeDocument(resultDocumentBuilder.build(), indexedDoc.build());
		}
		{
			int numberOfResults = 10;
			String normalLuceneQuery = "issn:1234-1234 AND title:special";
			QueryResponse qr = lumongoClient.query(normalLuceneQuery, numberOfResults, "myTestIndex");
			long totalHits = qr.getTotalHits();
			
			for (ScoredResult sr : qr.getResultsList()) {
				String uniqueId = sr.getUniqueId();
				float score = sr.getScore();
				System.out.println("Matching document <" + uniqueId + "> with score <" + score + ">");
			}
			
			System.out.println("Query <" + normalLuceneQuery + "> found <" + totalHits + "> total hits.  Fetched <" + qr.getResultsList().size()
					+ "> documents.");
		}
		
		{
			int numberOfResults = 10;
			String normalLuceneQuery = "title:special";
			QueryResponse first = lumongoClient.query(normalLuceneQuery, numberOfResults, "myTestIndex");
			@SuppressWarnings("unused")
			QueryResponse next = lumongoClient.query(normalLuceneQuery, numberOfResults, "myTestIndex", first);
			
		}
		
		{
			String uniqueIdToDelete = "someId";
			lumongoClient.delete(uniqueIdToDelete);
		}
		
		{
			String uniqueId = "someId";
			FetchResponse fr = lumongoClient.fetchDocument(uniqueId);
			if (!fr.hasResultDocument()) {
				ResultDocument rd = fr.getResultDocument();
				String contents = rd.toByteString().toStringUtf8();
				System.out.println("Document: " + contents);
			}
		}
	}
}
