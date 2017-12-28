package org.lumongo.server.connection;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.Logger;
import org.lumongo.cluster.message.InternalServiceGrpc;

import java.util.concurrent.TimeUnit;

public class InternalRpcConnection {

	private final static Logger log = Logger.getLogger(InternalRpcConnection.class);
	private final String memberAddress;
	private final int internalServicePort;

	private ManagedChannel channel;
	private InternalServiceGrpc.InternalServiceBlockingStub blockingStub;

	public InternalRpcConnection(String memberAddress, int internalServicePort) {
		this.memberAddress = memberAddress;
		this.internalServicePort = internalServicePort;

		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(memberAddress, internalServicePort)
				.maxInboundMessageSize(256 * 1024 * 1024).usePlaintext(true);
		channel = managedChannelBuilder.build();

		blockingStub = InternalServiceGrpc.newBlockingStub(channel);

		log.info("Connecting to <" + memberAddress + ":" + internalServicePort + ">");
	}

	public InternalServiceGrpc.InternalServiceBlockingStub getService() {
		return blockingStub;
	}

	public void close() {
		try {
			if (channel != null) {
				log.info("Closing connection to <" + memberAddress + ":" + internalServicePort + ">");
				channel.shutdown().awaitTermination(15, TimeUnit.SECONDS);
			}
		}
		catch (Exception e) {
			System.err.println("ERROR: Exception: " + e);
			e.printStackTrace();
		}
		channel = null;
		blockingStub = null;

	}
}
