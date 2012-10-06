package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.GetIndexesRequest;
import org.lumongo.cluster.message.Lumongo.GetIndexesResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GetIndexes extends Command<GetIndexesRequest, GetIndexesResult> {


	public GetIndexes() {

	}

	@Override
	public GetIndexesRequest getRequest() {
		return GetIndexesRequest.newBuilder().build();
	}

	@Override
	public GetIndexesResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		ExternalService.BlockingInterface service = lumongoConnection.getService();

		RpcController controller = lumongoConnection.getController();

		GetIndexesResponse getIndexesResponse = service.getIndexes(controller, getRequest());

		return new GetIndexesResult(getIndexesResponse);
	}






}
