package org.lumongo.server.connection;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.index.LumongoIndexManager;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Created by Matt Davis on 6/28/17.
 * @author mdavis
 */
public class ExternalServiceServer {

	private Server server;

	public ExternalServiceServer(LocalNodeConfig localNodeConfig, LumongoIndexManager indexManager) throws UnknownHostException {

		int externalServicePort = localNodeConfig.getExternalServicePort();

		ExternalServiceHandler externalServiceHandler = new ExternalServiceHandler(indexManager);
		server = NettyServerBuilder.forPort(externalServicePort).addService(externalServiceHandler).maxMessageSize(128 * 1024 * 1024).build();
	}

	public void start() throws IOException {
		server.start();
	}

	public void shutdown() {
		server.shutdown();

	}
}
