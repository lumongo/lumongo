package org.lumongo.client.pool;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.lumongo.client.LumongoRestClient;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.LMMember;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class LumongoConnection {

	private LMMember member;

	private ManagedChannel channel;

	private ExternalServiceGrpc.ExternalServiceBlockingStub blockingStub;
	private ExternalServiceGrpc.ExternalServiceStub asyncStub;

	public LumongoConnection(LMMember member) throws IOException {
		this.member = member;
	}

	public void open(boolean compressedConnection) throws IOException {

		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(member.getServerAddress(), member.getExternalPort())
				.usePlaintext(true);
		channel = managedChannelBuilder.build();

		blockingStub = ExternalServiceGrpc.newBlockingStub(channel);
		if (compressedConnection) {
			blockingStub = blockingStub.withCompression("gzip");
		}

		asyncStub = ExternalServiceGrpc.newStub(channel);
		if (compressedConnection) {
			asyncStub = asyncStub.withCompression("gzip");
		}

		System.err.println("INFO: Connecting to <" + member.getServerAddress() + ">");

	}

	public LumongoRestClient getRestClient() throws Exception {
		return new LumongoRestClient(member.getServerAddress(), member.getRestPort());
	}

	public ExternalServiceGrpc.ExternalServiceBlockingStub getService() {
		return blockingStub;
	}

	/**
	 * closes the connection to the server if open, calling a method (index, query, ...) will open a new connection
	 */
	public void close() {

		try {
			if (channel != null) {
				channel.shutdown().awaitTermination(15, TimeUnit.SECONDS);
			}
		}
		catch (Exception e) {
			System.err.println("ERROR: Exception: " + e);
			e.printStackTrace();
		}
		channel = null;
		blockingStub = null;
		asyncStub = null;

	}

}
