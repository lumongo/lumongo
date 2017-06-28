package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.GetIndexesRequest;
import org.lumongo.cluster.message.Lumongo.GetIndexesResponse;

public class GetIndexes extends SimpleCommand<GetIndexesRequest, GetIndexesResult> {

	public GetIndexes() {

	}

	@Override
	public GetIndexesRequest getRequest() {
		return GetIndexesRequest.newBuilder().build();
	}

	@Override
	public GetIndexesResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		GetIndexesResponse getIndexesResponse = service.getIndexes(getRequest());

		return new GetIndexesResult(getIndexesResponse);
	}

}
