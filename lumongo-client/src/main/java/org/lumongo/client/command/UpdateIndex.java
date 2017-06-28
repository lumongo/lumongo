package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.UpdateIndexResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.IndexSettingsRequest;
import org.lumongo.cluster.message.Lumongo.IndexSettingsResponse;

public class UpdateIndex extends SimpleCommand<IndexSettingsRequest, UpdateIndexResult> {

	private IndexConfig indexConfig;
	private String indexName;

	public UpdateIndex(String indexName, IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
		this.indexName = indexName;
	}

	@Override
	public IndexSettingsRequest getRequest() {
		IndexSettingsRequest.Builder indexSettingsRequestBuilder = IndexSettingsRequest.newBuilder();
		indexSettingsRequestBuilder.setIndexName(indexName);
		if (indexConfig != null) {
			indexSettingsRequestBuilder.setIndexSettings(indexConfig.getIndexSettings());
		}

		return indexSettingsRequestBuilder.build();
	}

	@Override
	public UpdateIndexResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		IndexSettingsResponse indexSettingsResponse = service.changeIndex(getRequest());

		return new UpdateIndexResult(indexSettingsResponse);
	}

}
