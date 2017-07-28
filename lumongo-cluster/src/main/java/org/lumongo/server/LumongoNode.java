package org.lumongo.server;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import org.apache.log4j.Logger;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.config.Nodes;
import org.lumongo.server.connection.ExternalServiceServer;
import org.lumongo.server.connection.InternalServiceServer;
import org.lumongo.server.hazelcast.HazelcastManager;
import org.lumongo.server.index.LumongoIndexManager;
import org.lumongo.server.rest.RestServiceManager;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LogUtil;

import java.io.IOException;

public class LumongoNode {
	private final static Logger log = Logger.getLogger(LumongoNode.class);

	static {
		try {
			LogUtil.loadLogConfig();
		}
		catch (Exception e) {
			throw new RuntimeException();
		}
	}

	private final ExternalServiceServer externalServiceServer;
	private final InternalServiceServer internalServiceServer;
	private final LumongoIndexManager indexManager;
	private final HazelcastManager hazelcastManager;

	private final RestServiceManager restServiceManager;

	public LumongoNode(MongoConfig mongoConfig, String localServer, int instance) throws Exception {

		log.info("Using mongo <" + mongoConfig.getServerAddresses() + ">");
		MongoClient mongo = new MongoClient(mongoConfig.getServerAddresses());

		ClusterHelper clusterHelper = new ClusterHelper(mongo, mongoConfig.getDatabaseName());

		LocalNodeConfig localNodeConfig = clusterHelper.getNodeConfig(localServer, instance);

		ClusterConfig clusterConfig = clusterHelper.getClusterConfig();

		log.info("Loaded cluster config: <" + clusterConfig + ">");

		MongoDirectory.setMaxIndexBlocks(clusterConfig.getMaxIndexBlocks());

		this.indexManager = new LumongoIndexManager(mongo, mongoConfig, clusterConfig);

		this.externalServiceServer = new ExternalServiceServer(localNodeConfig, indexManager);
		this.internalServiceServer = new InternalServiceServer(localNodeConfig, indexManager);

		if (localNodeConfig.hasRestPort()) {
			this.restServiceManager = new RestServiceManager(localNodeConfig, indexManager);
		}
		else {
			this.restServiceManager = null;
		}

		Nodes nodes = clusterHelper.getNodes();
		this.hazelcastManager = HazelcastManager
				.createHazelcastManager(localNodeConfig, indexManager, nodes.getHazelcastNodes(), mongoConfig.getDatabaseName());

	}

	public void start() throws MongoException, IOException {

		internalServiceServer.start();
		externalServiceServer.start();
		if (restServiceManager != null) {
			restServiceManager.start();
		}
	}

	public void setupShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread("ShutdownThread-" + hazelcastManager.getHazelcastPort()) {
			@Override
			public void run() {
				shutdown();
			}
		});
	}

	public void shutdown() {

		externalServiceServer.shutdown();
		internalServiceServer.shutdown();
		if (restServiceManager != null) {
			restServiceManager.shutdown();
		}

		indexManager.shutdown();

		hazelcastManager.shutdown();

	}
}
