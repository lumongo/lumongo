package org.lumongo.server;

import com.mongodb.MongoException;
import org.apache.log4j.Logger;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.config.Nodes;
import org.lumongo.server.connection.ExternalServiceHandler;
import org.lumongo.server.connection.InternalServiceHandler;
import org.lumongo.server.hazelcast.HazelcastManager;
import org.lumongo.server.indexing.LumongoIndexManager;
import org.lumongo.server.rest.RestServiceManager;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LogUtil;

import java.io.IOException;

public class LuceneNode {
	private final static Logger log = Logger.getLogger(LuceneNode.class);

	static {
		try {
			LogUtil.loadLogConfig();
		}
		catch (Exception e) {
			throw new RuntimeException();
		}
	}

	private final ExternalServiceHandler externalServiceHandler;
	private final InternalServiceHandler internalServiceHandler;
	private final LumongoIndexManager indexManager;
	private final HazelcastManager hazelcastManager;

	private final RestServiceManager restServiceManager;

	public LuceneNode(MongoConfig mongoConfig, String localServer, int instance) throws Exception {

		LocalNodeConfig localNodeConfig = ClusterHelper.getNodeConfig(mongoConfig, localServer, instance);

		ClusterConfig clusterConfig = ClusterHelper.getClusterConfig(mongoConfig);

		log.info("Loaded cluster config: <" + clusterConfig + ">");

		MongoDirectory.setMaxIndexBlocks(clusterConfig.getMaxIndexBlocks());

		this.indexManager = new LumongoIndexManager(mongoConfig, clusterConfig);

		this.externalServiceHandler = new ExternalServiceHandler(clusterConfig, localNodeConfig, indexManager);
		this.internalServiceHandler = new InternalServiceHandler(clusterConfig, localNodeConfig, indexManager);

		if (localNodeConfig.hasRestPort()) {
			this.restServiceManager = new RestServiceManager(localNodeConfig, indexManager);
		}
		else {
			this.restServiceManager = null;
		}

		Nodes nodes = ClusterHelper.getNodes(mongoConfig);
		this.hazelcastManager = HazelcastManager
						.createHazelcastManager(localNodeConfig, indexManager, nodes.getHazelcastNodes(), mongoConfig.getDatabaseName());

	}

	public void start() throws MongoException, IOException {

		internalServiceHandler.start();
		externalServiceHandler.start();
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

		externalServiceHandler.shutdown();
		internalServiceHandler.shutdown();
		if (restServiceManager != null) {
			restServiceManager.shutdown();
		}

		indexManager.shutdown();

		hazelcastManager.shutdown();


	}
}
