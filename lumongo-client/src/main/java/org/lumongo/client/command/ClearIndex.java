package org.lumongo.client.command;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.ClearIndexResult;
import org.lumongo.cluster.message.Lumongo.ClearRequest;
import org.lumongo.cluster.message.Lumongo.ClearResponse;
import org.lumongo.cluster.message.Lumongo.ExternalService;

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
	public ClearIndexResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		ExternalService.BlockingInterface service = lumongoConnection.getService();
		RpcController controller = lumongoConnection.getController();

		ClearResponse clearResponse = service.clear(controller, getRequest());

		return new ClearIndexResult(clearResponse);
	}

}
