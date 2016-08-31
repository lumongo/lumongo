package org.lumongo.client.pool;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientPipelineFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.lumongo.client.LumongoRestClient;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.LMMember;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class LumongoConnection {

	private static CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();

	private LMMember member;
	private ExternalService.BlockingInterface service;
	private RpcClient rpcClient;
	private Bootstrap bootstrap;

	public LumongoConnection(LMMember member) throws IOException {
		this.member = member;
	}

	public void open(boolean compressedConnection) throws IOException {

		PeerInfo server = new PeerInfo(member.getServerAddress(), member.getExternalPort());
		//PeerInfo client = new PeerInfo(myHostName + "-" + UUID.randomUUID().toString(), 1234);

		System.err.println("INFO: Connecting to <" + server + ">");

		DuplexTcpClientPipelineFactory clientFactory = new DuplexTcpClientPipelineFactory();
		clientFactory.setCompression(compressedConnection);
		clientFactory.setRpcLogger(null);

		this.bootstrap = new Bootstrap();
		bootstrap.group(new NioEventLoopGroup());
		bootstrap.handler(clientFactory);
		bootstrap.channel(NioSocketChannel.class);

		//TODO check this options
		bootstrap.option(ChannelOption.TCP_NODELAY, true);
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
		bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
		bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);

		shutdownHandler.addResource(bootstrap.group());

		rpcClient = clientFactory.peerWith(server, bootstrap);

		service = ExternalService.newBlockingStub(rpcClient);

	}

	public LumongoRestClient getRestClient() throws Exception {
		return new LumongoRestClient(member.getServerAddress(), member.getRestPort());
	}

	public RpcController getController() {
		return rpcClient.newRpcController();
	}

	public ExternalService.BlockingInterface getService() {
		return service;
	}

	/**
	 * closes the connection to the server if open, calling a method (index, query, ...) will open a new connection
	 */
	public void close() {
		try {

			if (rpcClient != null) {
				System.err.println("INFO: Closing connection to " + rpcClient.getPeerInfo());
				rpcClient.close();
			}
		}
		catch (Exception e) {
			System.err.println("ERROR: Exception: " + e);
			e.printStackTrace();
		}
		rpcClient = null;
		try {
			if (bootstrap != null) {
				bootstrap.group().shutdownGracefully(0, 15, TimeUnit.SECONDS);
				shutdownHandler.removeResource(bootstrap.group());
			}
		}
		catch (Exception e) {
			System.err.println("ERROR: Exception: " + e);
			e.printStackTrace();
		}
		bootstrap = null;
		service = null;
	}

}
