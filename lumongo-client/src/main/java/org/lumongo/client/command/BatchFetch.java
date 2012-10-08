package org.lumongo.client.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;
import org.lumongo.cluster.message.Lumongo.GroupFetchRequest;
import org.lumongo.cluster.message.Lumongo.GroupFetchResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class BatchFetch extends SimpleCommand<GroupFetchRequest, BatchFetchResult> {

	private List<Fetch> fetchList;

	public BatchFetch(Collection<String> uniqueIds) {
		this.fetchList = new ArrayList<Fetch>();

		for (String uniqueId : uniqueIds) {
			Fetch f = new Fetch(uniqueId);
			f.setResultFetchType(FetchType.FULL);
			f.setAssociatedFetchType(FetchType.NONE);
			fetchList.add(f);
		}
	}

	public BatchFetch(List<Fetch> fetchList) {
		this.fetchList = fetchList;
	}

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
