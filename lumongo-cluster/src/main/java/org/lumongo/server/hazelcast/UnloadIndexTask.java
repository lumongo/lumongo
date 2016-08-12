package org.lumongo.server.hazelcast;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class UnloadIndexTask implements Callable<Void>, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private final int hazelcastPort;
	
	private final String indexName;
	private final boolean delete;

	public UnloadIndexTask(int hazelcastPort, String indexName, boolean delete) {
		this.hazelcastPort = hazelcastPort;
		this.indexName = indexName;
		this.delete = delete;
	}
	
	@Override
	public Void call() throws Exception {

		HazelcastManager.getHazelcastManager(hazelcastPort).unloadIndex(indexName, delete);
		
		return null;
	}
}
