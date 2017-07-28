package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.ClearIndexResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.ClearRequest;
import org.lumongo.cluster.message.Lumongo.ClearResponse;

/**
 * Removes all documents from a given index
 * @author mdavis
 *
 */
public class ClearIndex extends SimpleCommand<ClearRequest, ClearIndexResult> {

	private String indexName;

	public ClearIndex(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public ClearRequest getRequest() {
		return ClearRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public ClearIndexResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		ClearResponse clearResponse = service.clear(getRequest());

		return new ClearIndexResult(clearResponse);
	}

}
