package org.lumongo.server;

import java.net.UnknownHostException;

import org.jboss.netty.channel.ChannelException;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.config.Nodes;
import org.lumongo.server.connection.ExternalServiceHandler;
import org.lumongo.server.connection.InternalServiceHandler;
import org.lumongo.server.hazelcast.HazelcastManager;
import org.lumongo.server.indexing.IndexManager;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LogUtil;

import com.mongodb.MongoException;

public class LuceneNode {
	
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
	private final IndexManager indexManager;
	private final HazelcastManager hazelcastManager;
	
	public LuceneNode(MongoConfig mongoConfig, String localServer, int instance) throws Exception {
		
		LocalNodeConfig localNodeConfig = ClusterHelper.getNodeConfig(mongoConfig, localServer, instance);
		
		ClusterConfig clusterConfig = ClusterHelper.getClusterConfig(mongoConfig);
		
		int maxDirtyIndexBlocks = clusterConfig.getMaxDirtyIndexBlocks();
		MongoDirectory.setMaxDirtyIndexBlocks(maxDirtyIndexBlocks);
		
		this.indexManager = new IndexManager(mongoConfig, clusterConfig);
		
		this.externalServiceHandler = new ExternalServiceHandler(clusterConfig, localNodeConfig, indexManager);
		this.internalServiceHandler = new InternalServiceHandler(clusterConfig, localNodeConfig, indexManager);
		
		Nodes nodes = ClusterHelper.getNodes(mongoConfig);
		this.hazelcastManager = HazelcastManager.createHazelcastManager(localNodeConfig, indexManager, nodes.getHazelcastNodes(), mongoConfig.getDatabaseName());
		
	}
	
	public void start() throws ChannelException, UnknownHostException, MongoException {
		
		internalServiceHandler.start();
		externalServiceHandler.start();
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
		indexManager.shutdown();
		
		hazelcastManager.shutdown();
		
	}
}
