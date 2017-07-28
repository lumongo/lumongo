package org.lumongo.server.connection;

import org.apache.commons.pool.BasePoolableObjectFactory;

public class InternalRpcConnectionFactory extends BasePoolableObjectFactory<InternalRpcConnection> {

	private String memberAddress;
	private int internalServicePort;

	public InternalRpcConnectionFactory(String memberAddress, int internalServicePort) {
		this.memberAddress = memberAddress;
		this.internalServicePort = internalServicePort;
	}

	@Override
	public InternalRpcConnection makeObject() throws Exception {
		return new InternalRpcConnection(memberAddress, internalServicePort);
	}

	@Override
	public void destroyObject(InternalRpcConnection obj) throws Exception {
		obj.close();
	}

}