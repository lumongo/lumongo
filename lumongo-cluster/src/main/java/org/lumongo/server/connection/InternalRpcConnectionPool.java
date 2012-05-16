package org.lumongo.server.connection;

import org.apache.commons.pool.impl.GenericObjectPool;

public class InternalRpcConnectionPool extends GenericObjectPool<InternalRpcConnection> {
	
	public InternalRpcConnectionPool(String memberAddress, int internalServicePort, int maxConnections) {
		super(new InternalRpcConnectionFactory(memberAddress, internalServicePort), 32 * 1024);
		setMaxIdle(maxConnections);
		setMinEvictableIdleTimeMillis(1000L * 300L);
	}
}
