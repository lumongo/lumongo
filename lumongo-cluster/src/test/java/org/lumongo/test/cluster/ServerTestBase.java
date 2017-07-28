package org.lumongo.test.cluster;

import com.mongodb.MongoClient;
import org.apache.log4j.Logger;
import org.lumongo.LumongoConstants;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.pool.LumongoWorkPool;
import org.lumongo.server.LumongoNode;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LogUtil;
import org.lumongo.util.ServerNameHelper;
import org.lumongo.util.TestHelper;
import org.lumongo.util.properties.FakePropertiesReader;
import org.lumongo.util.properties.PropertiesReader.PropertyException;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ServerTestBase {
	private static Logger log = Logger.getLogger(ServerTestBase.class);
	
	private LumongoWorkPool lumongoWorkPool;
	private List<LumongoNode> luceneNodes;
	
	public void startSuite(int instanceCount) throws Exception {
		
		luceneNodes = new ArrayList<>();
		
		Thread.currentThread().setName(this.getClass().getSimpleName());
		
		LogUtil.loadLogConfig();


		removeTestDBs();

		startServer(instanceCount);
		startClient();
	}

	public void stopSuite() throws Exception {
		stopClient();
		stopServer();

		removeTestDBs();

	}

	private void removeTestDBs() throws UnknownHostException {
		log.info("Removing test databases");
		MongoClient mongo = TestHelper.getMongo();

		for (String dbName : mongo.listDatabaseNames()) {
			if (dbName.startsWith(TestHelper.TEST_DATABASE_NAME)) {
				mongo.getDatabase(dbName).drop();
			}
		}
	}

	public void startServer(int instanceCount) throws Exception {
		
		log.info("Starting server");
		
		MongoConfig mongoConfig = getTestMongoConfig();
		
		ClusterConfig clusterConfig = getTestClusterConfig();
		ClusterHelper clusterHelper = new ClusterHelper(mongoConfig);
		clusterHelper.saveClusterConfig(clusterConfig);
		
		String localServer = ServerNameHelper.getLocalServer();

		for (int i = 0; i < instanceCount; i++) {
			LumongoNode ln = createLuceneNode(clusterHelper, mongoConfig, localServer, i);
			ln.start();
			luceneNodes.add(ln);
		}
		

		
	}
	
	public LumongoNode createLuceneNode(ClusterHelper clusterHelper, MongoConfig mongoConfig, String localServer, int instance) throws Exception {
		LocalNodeConfig localNodeConfig = getTestLocalNodeConfig(instance);
		clusterHelper.registerNode(localNodeConfig, localServer);
		return new LumongoNode(mongoConfig, localServer, localNodeConfig.getHazelcastPort());
	}
	
	public void stopServer() throws Exception {
		log.info("Stopping server");
		for (LumongoNode ln : luceneNodes) {
			ln.shutdown();
		}
		luceneNodes.clear();
	}
	
	public ClusterConfig getTestClusterConfig() throws PropertyException {
		HashMap<String, String> settings = new HashMap<>();
		
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
	
	public LocalNodeConfig getTestLocalNodeConfig(int instance) throws PropertyException {
		int offset = (instance * 10);
		
		HashMap<String, String> settings = new HashMap<>();
		
		settings.put(LocalNodeConfig.HAZELCAST_PORT, (LumongoConstants.DEFAULT_HAZELCAST_PORT + offset) + "");
		settings.put(LocalNodeConfig.INTERNAL_SERVICE_PORT, (LumongoConstants.DEFAULT_INTERNAL_SERVICE_PORT + offset) + "");
		settings.put(LocalNodeConfig.EXTERNAL_SERVICE_PORT, (LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT + offset) + "");
		settings.put(LocalNodeConfig.REST_PORT, (LumongoConstants.DEFAULT_REST_SERVICE_PORT + offset) + "");
		LocalNodeConfig localNodeConfig = new LocalNodeConfig(new FakePropertiesReader("test", settings));
		return localNodeConfig;
	}
	
	public MongoConfig getTestMongoConfig() throws PropertyException {
		HashMap<String, String> settings = new HashMap<>();
		
		settings.put(MongoConfig.DATABASE_NAME, TestHelper.TEST_DATABASE_NAME);
		settings.put(MongoConfig.MONGO_HOSTS, TestHelper.getMongoServer());
		
		MongoConfig mongoConfig = new MongoConfig(new FakePropertiesReader("test", settings));
		return mongoConfig;
	}
	
	public void startClient() throws Exception {
		System.out.println("Starting client");
		LumongoPoolConfig lumongoPoolConfig = new LumongoPoolConfig();
		lumongoPoolConfig.addMember("localhost");
		lumongoWorkPool = new LumongoWorkPool(lumongoPoolConfig);
		lumongoWorkPool.updateMembers();
	}
	
	public void stopClient() throws Exception {
		lumongoWorkPool.shutdown();
	}
	
	public LumongoWorkPool getLumongoWorkPool() {
		return lumongoWorkPool;
	}
	
}
