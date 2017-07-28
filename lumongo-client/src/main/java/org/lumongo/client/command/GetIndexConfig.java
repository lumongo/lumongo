package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo;

/**
 * Created by Payam Meyer on 4/3/17.
 * @author pmeyer
 */
public class GetIndexConfig extends SimpleCommand<Lumongo.GetIndexConfigRequest, GetIndexConfigResult> {

	private final String indexName;

	public GetIndexConfig(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public Lumongo.GetIndexConfigRequest getRequest() {
		return Lumongo.GetIndexConfigRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public GetIndexConfigResult execute(LumongoConnection lumongoConnection) {

		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		Lumongo.GetIndexConfigResponse getIndexConfigResponse = service.getIndexConfig(getRequest());

		IndexConfig indexConfig = new IndexConfig();
		indexConfig.configure(getIndexConfigResponse.getIndexSettings());

		return new GetIndexConfigResult(indexConfig);
	}
}
