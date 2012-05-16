package org.lumongo.server.connection;

import java.util.UUID;
import java.util.concurrent.Executors;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.lumongo.cluster.message.Lumongo.InternalService;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;

public class InternalRpcConnectionFactory extends BasePoolableObjectFactory<InternalRpcConnection> {
	private final static Logger log = Logger.getLogger(InternalRpcConnectionFactory.class);
	
	private String memberAddress;
	private int internalServicePort;
	
	public InternalRpcConnectionFactory(String memberAddress, int internalServicePort) {
		this.memberAddress = memberAddress;
		this.internalServicePort = internalServicePort;
	}
	
	@Override
	public InternalRpcConnection makeObject() throws Exception {
		PeerInfo server = new PeerInfo(memberAddress, internalServicePort);
		
		PeerInfo client = new PeerInfo(ConnectionHelper.getHostName() + "-" + UUID.randomUUID().toString(), 4321);
		
		log.info("Connecting from <" + client + "> to <" + server + ">");
		
		// should never be used here because we are just doing server side calls
		ThreadPoolCallExecutor executor = new ThreadPoolCallExecutor(1, 1);
		
		DuplexTcpClientBootstrap bootstrap = new DuplexTcpClientBootstrap(client, new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()), executor);
		bootstrap.setRpcLogger(null);
		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("connectResponseTimeoutMillis", 10000);
		bootstrap.setOption("receiveBufferSize", 1048576);
		bootstrap.setOption("tcpNoDelay", false);
		
		RpcClient rpcClient = bootstrap.peerWith(server);
		
		InternalService.BlockingInterface service = InternalService.newBlockingStub(rpcClient);
		
		return new InternalRpcConnection(service, rpcClient, bootstrap);
	}
	
	@Override
	public void destroyObject(InternalRpcConnection obj) throws Exception {
		obj.close();
	}
	
}