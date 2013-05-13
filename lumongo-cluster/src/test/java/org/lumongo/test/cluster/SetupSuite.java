package org.lumongo.test.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.lumongo.LumongoConstants;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.server.LuceneNode;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.indexing.Index;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LogUtil;
import org.lumongo.util.ServerNameHelper;
import org.lumongo.util.TestHelper;
import org.lumongo.util.properties.FakePropertiesReader;
import org.lumongo.util.properties.PropertiesReader.PropertyException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.log4testng.Logger;

import com.mongodb.Mongo;

public class SetupSuite {
	private static Logger log = Logger.getLogger(SetupSuite.class);

	private static LumongoWorkPool lumongoWorkPool;
	private static List<LuceneNode> luceneNodes;

	@BeforeSuite
	public static void startSuite() throws Exception {

		luceneNodes = new ArrayList<LuceneNode>();

		Thread.currentThread().setName("Test");

		LogUtil.loadLogConfig();

		log.info("Cleaning existing test dbs and initing");

		Mongo mongo = TestHelper.getMongo();
		mongo.getDB(TestHelper.TEST_DATABASE_NAME).dropDatabase();
		mongo.getDB(TestHelper.TEST_DATABASE_NAME + Index.STORAGE_DB_SUFFIX).dropDatabase();

		startServer();
	}

	@AfterSuite
	public static void stopSuite() throws Exception {
		stopClient();
		stopServer();
	}

	public static void startServer() throws Exception {

		log.info("Starting server for single node test");

		MongoConfig mongoConfig = getTestMongoConfig();

		ClusterConfig clusterConfig = getTestClusterConfig();
		ClusterHelper.saveClusterConfig(mongoConfig, clusterConfig);

		String localServer = ServerNameHelper.getLocalServer();

		int instances = 1;
		for (int i = 0; i < instances; i++) {
			LuceneNode ln = createLuceneNode(mongoConfig, localServer, i);
			ln.start();
			luceneNodes.add(ln);
		}

		startClient();


	}

	public static LuceneNode createLuceneNode(MongoConfig mongoConfig, String localServer, int instance) throws PropertyException, Exception {
		LocalNodeConfig localNodeConfig = getTestLocalNodeConfig(instance);
		ClusterHelper.registerNode(mongoConfig, localNodeConfig, localServer);
		return new LuceneNode(mongoConfig, localServer, localNodeConfig.getHazelcastPort());
	}

	public static void stopServer() throws Exception {
		log.info("Stopping server for single node test");
		for (LuceneNode ln : luceneNodes) {
			ln.shutdown();
		}
	}

	public static ClusterConfig getTestClusterConfig() throws PropertyException {
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

	public static LocalNodeConfig getTestLocalNodeConfig(int instance) throws PropertyException {
		int offset = instance * 10;

		HashMap<String, String> settings = new HashMap<String, String>();

		settings.put(LocalNodeConfig.HAZELCAST_PORT, (LumongoConstants.DEFAULT_HAZELCAST_PORT + offset) + "");
		settings.put(LocalNodeConfig.INTERNAL_SERVICE_PORT, (LumongoConstants.DEFAULT_INTERNAL_SERVICE_PORT + offset) + "");
		settings.put(LocalNodeConfig.EXTERNAL_SERVICE_PORT, (LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT + offset) + "");
		settings.put(LocalNodeConfig.REST_PORT, (LumongoConstants.DEFAULT_REST_SERVICE_PORT + offset) + "");
		LocalNodeConfig localNodeConfig = new LocalNodeConfig(new FakePropertiesReader("test", settings));
		return localNodeConfig;
	}

	public static MongoConfig getTestMongoConfig() throws PropertyException {
		HashMap<String, String> settings = new HashMap<String, String>();

		settings.put(MongoConfig.DATABASE_NAME, TestHelper.TEST_DATABASE_NAME);
		settings.put(MongoConfig.MONGO_HOST, TestHelper.getMongoServer());
		settings.put(MongoConfig.MONGO_PORT, String.valueOf(TestHelper.getMongoPort()));

		MongoConfig mongoConfig = new MongoConfig(new FakePropertiesReader("test", settings));
		return mongoConfig;
	}

	public static void startClient() throws Exception {
		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
		lumongoPoolConfig.addMember("localhost");
		lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);
		lumongoWorkPool.updateMembers();
	}

	public static void stopClient() throws Exception {
		lumongoWorkPool.shutdown();
	}

	public static LumongoWorkPool getLumongoWorkPool() {
		return lumongoWorkPool;
	}

}
