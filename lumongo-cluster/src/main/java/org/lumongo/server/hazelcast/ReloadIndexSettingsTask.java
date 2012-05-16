package org.lumongo.server.hazelcast;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class ReloadIndexSettingsTask implements Callable<Void>, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private final int hazelcastPort;
	
	private final String indexName;
	
	public ReloadIndexSettingsTask(int hazelcastPort, String indexName) {
		this.hazelcastPort = hazelcastPort;
		this.indexName = indexName;
	}
	
	@Override
	public Void call() throws Exception {
		
		HazelcastManager.getHazelcastManager(hazelcastPort).reloadIndexSettings(indexName);
		
		return null;
	}
}
