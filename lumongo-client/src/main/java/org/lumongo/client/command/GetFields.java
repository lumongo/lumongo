package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetFieldsResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesRequest;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;

/**
 * Returns all the fields from a given index
 * @author mdavis
 *
 */
public class GetFields extends SimpleCommand<GetFieldNamesRequest, GetFieldsResult> {

	private String indexName;

	public GetFields(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public GetFieldNamesRequest getRequest() {
		return GetFieldNamesRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public GetFieldsResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		GetFieldNamesResponse getFieldNamesResponse = service.getFieldNames(getRequest());

		return new GetFieldsResult(getFieldNamesResponse);
	}

}
