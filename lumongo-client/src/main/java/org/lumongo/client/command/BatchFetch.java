package org.lumongo.client.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo.BatchFetchRequest;
import org.lumongo.cluster.message.Lumongo.BatchFetchResponse;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FetchType;
import org.lumongo.cluster.message.Lumongo.ScoredResult;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Fetches multiple documents in a single call
 * @author mdavis
 *
 */
public class BatchFetch extends SimpleCommand<BatchFetchRequest, BatchFetchResult> {
	
	private List<Fetch> fetchList;
	
	public BatchFetch() {
		this.fetchList = new ArrayList<Fetch>();
	}
	
	public BatchFetch addFetches(Collection<? extends Fetch> fetches) {
		this.fetchList.addAll(fetches);
		return this;
	}
	
	public BatchFetch addFetchDocumentsFromUniqueIds(Collection<String> uniqueIds, String indexName) {
		
		for (String uniqueId : uniqueIds) {
			Fetch f = new Fetch(uniqueId, indexName);
			f.setResultFetchType(FetchType.FULL);
			f.setAssociatedFetchType(FetchType.NONE);
			fetchList.add(f);
		}
		return this;
	}
	
	public BatchFetch addFetchDocumentsFromResults(QueryResult qr) {
		return addFetchDocumentsFromResults(qr.getResults());
	}
	
	public BatchFetch addFetchDocumentsFromResults(Collection<ScoredResult> scoredResults) {
		
		for (ScoredResult scoredResult : scoredResults) {
			Fetch f = new Fetch(scoredResult.getUniqueId(), scoredResult.getIndexName());
			f.setResultFetchType(FetchType.FULL);
			f.setAssociatedFetchType(FetchType.NONE);
			fetchList.add(f);
		}
		return this;
	}
	
	@Override
	public BatchFetchRequest getRequest() {
		BatchFetchRequest.Builder batchFetchRequestBuilder = BatchFetchRequest.newBuilder();
		for (Fetch f : fetchList) {
			batchFetchRequestBuilder.addFetchRequest(f.getRequest());
		}
		return batchFetchRequestBuilder.build();
	}
	
	@Override
	public BatchFetchResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		ExternalService.BlockingInterface service = lumongoConnection.getService();
		RpcController controller = lumongoConnection.getController();
		
		BatchFetchResponse batchFetchResponse = service.batchFetch(controller, getRequest());
		
		return new BatchFetchResult(batchFetchResponse);
	}
	
}
