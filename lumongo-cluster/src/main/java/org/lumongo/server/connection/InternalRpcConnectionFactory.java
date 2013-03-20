package org.lumongo.server.connection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.UUID;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;
import org.lumongo.cluster.message.Lumongo.InternalService;
import org.lumongo.cluster.message.Lumongo.InternalService.BlockingInterface;

import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientPipelineFactory;

public class InternalRpcConnectionFactory extends BasePoolableObjectFactory<InternalRpcConnection> {
	private static CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();

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


		DuplexTcpClientPipelineFactory clientFactory = new DuplexTcpClientPipelineFactory(client);
		clientFactory.setCompression(false);
		clientFactory.setRpcLogger(null);

		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(new NioEventLoopGroup());
		bootstrap.handler(clientFactory);
		bootstrap.channel(NioSocketChannel.class);

		//TODO check this options
		bootstrap.option(ChannelOption.TCP_NODELAY, true);
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,10000);
		bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
		bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);

		shutdownHandler.addResource(bootstrap);

		RpcClient rpcClient = clientFactory.peerWith(server, bootstrap);

		BlockingInterface service = InternalService.newBlockingStub(rpcClient);

		return new InternalRpcConnection(service, rpcClient, bootstrap);
	}

	@Override
	public void destroyObject(InternalRpcConnection obj) throws Exception {
		obj.close();
	}

}