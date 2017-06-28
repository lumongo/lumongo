package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetNumberOfDocsResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsRequest;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;

public class GetNumberOfDocs extends SimpleCommand<GetNumberOfDocsRequest, GetNumberOfDocsResult> {

	private String indexName;

	public GetNumberOfDocs(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public GetNumberOfDocsRequest getRequest() {
		return GetNumberOfDocsRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public GetNumberOfDocsResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		GetNumberOfDocsResponse getNumberOfDocsResponse = service.getNumberOfDocs(getRequest());

		return new GetNumberOfDocsResult(getNumberOfDocsResponse);
	}

}
