package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.DeleteIndexResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.IndexDeleteRequest;
import org.lumongo.cluster.message.Lumongo.IndexDeleteResponse;

/**
 * Deletes an index.  If index does not exist throwns an exception
 * @author mdavis
 *
 */
public class DeleteIndex extends SimpleCommand<IndexDeleteRequest, DeleteIndexResult> {

	private String indexName;

	public DeleteIndex(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public IndexDeleteRequest getRequest() {
		return IndexDeleteRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public DeleteIndexResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		IndexDeleteResponse indexDeleteResponse = service.deleteIndex(getRequest());

		return new DeleteIndexResult(indexDeleteResponse);
	}

}
