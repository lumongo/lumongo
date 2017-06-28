package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.BatchDeleteResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.BatchDeleteRequest;
import org.lumongo.cluster.message.Lumongo.BatchDeleteResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;

import java.util.ArrayList;
import java.util.List;

public class BatchDelete extends SimpleCommand<BatchDeleteRequest, BatchDeleteResult> {

	private List<Delete> deletes;

	public BatchDelete() {
		deletes = new ArrayList<Delete>();
	}

	public BatchDelete addDelete(Delete delete) {
		deletes.add(delete);
		return this;
	}

	public BatchDelete deleteDocumentFromQueryResult(QueryResult queryResult) {

		for (ScoredResult sr : queryResult.getResults()) {
			Delete delete = new DeleteDocument(sr.getUniqueId(), sr.getIndexName());
			deletes.add(delete);
		}

		return this;
	}

	@Override
	public BatchDeleteRequest getRequest() {
		BatchDeleteRequest.Builder batchDeleteRequest = BatchDeleteRequest.newBuilder();

		for (Delete delete : deletes) {
			batchDeleteRequest.addRequest(delete.getRequest());
		}

		return batchDeleteRequest.build();
	}

	@Override
	public BatchDeleteResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		BatchDeleteResponse batchDeleteResponse = service.batchDelete(getRequest());

		return new BatchDeleteResult(batchDeleteResponse);
	}

}
