package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.OptimizeIndexResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.OptimizeRequest;
import org.lumongo.cluster.message.Lumongo.OptimizeResponse;

/**
 * Optimizes a given index
 * @author mdavis
 *
 */
public class OptimizeIndex extends SimpleCommand<OptimizeRequest, OptimizeIndexResult> {

	private String indexName;

	public OptimizeIndex(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public OptimizeRequest getRequest() {
		return OptimizeRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public OptimizeIndexResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		OptimizeResponse optimizeResponse = service.optimize(getRequest());

		return new OptimizeIndexResult(optimizeResponse);
	}

}
