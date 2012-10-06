package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.OptimizeResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.OptimizeRequest;
import org.lumongo.cluster.message.Lumongo.OptimizeResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class OptimizeIndex extends Command<OptimizeRequest, OptimizeResult> {

	private String indexName;

	public OptimizeIndex(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public OptimizeRequest getRequest() {
		return OptimizeRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public OptimizeResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		ExternalService.BlockingInterface service = lumongoConnection.getService();

		RpcController controller = lumongoConnection.getController();

		OptimizeResponse optimizeResponse = service.optimize(controller, getRequest());

		return new OptimizeResult(optimizeResponse);
	}



}
