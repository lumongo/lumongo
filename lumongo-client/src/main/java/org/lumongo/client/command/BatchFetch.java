package org.lumongo.client.command;

import java.util.List;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.GroupFetchRequest;
import org.lumongo.cluster.message.Lumongo.GroupFetchResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class BatchFetch extends SimpleCommand<GroupFetchRequest, BatchFetchResult> {

	private List<Fetch> fetchList;

	@Override
	public GroupFetchRequest getRequest() {
		GroupFetchRequest.Builder groupFetchRequestBuilder = GroupFetchRequest.newBuilder();
		for (Fetch f : fetchList) {
			groupFetchRequestBuilder.addFetchRequest(f.getRequest());
		}
		return groupFetchRequestBuilder.build();
	}

	@Override
	public BatchFetchResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		ExternalService.BlockingInterface service = lumongoConnection.getService();
		RpcController controller = lumongoConnection.getController();

		GroupFetchResponse groupFetchResponse = service.groupFetch(controller, getRequest());

		return new BatchFetchResult(groupFetchResponse);
	}

}
