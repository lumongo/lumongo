package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.BatchFetchRequest;
import org.lumongo.cluster.message.Lumongo.BatchFetchResponse;
import org.lumongo.cluster.message.Lumongo.FetchType;
import org.lumongo.cluster.message.Lumongo.ScoredResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Fetches multiple documents in a single call
 * @author mdavis
 *
 */
public class BatchFetch extends SimpleCommand<BatchFetchRequest, BatchFetchResult> {

	private List<Fetch> fetchList;

	public BatchFetch() {
		this.fetchList = new ArrayList<>();
	}

	public BatchFetch addFetches(Collection<? extends Fetch> fetches) {
		this.fetchList.addAll(fetches);
		return this;
	}

	public BatchFetch addFetchDocumentsFromUniqueIds(Collection<String> uniqueIds, String indexName) {
		return addFetchDocumentsFromUniqueIds(uniqueIds, indexName, null);
	}

	public BatchFetch addFetchDocumentsFromUniqueIds(Collection<String> uniqueIds, String indexName, Collection<String> documentFields) {

		for (String uniqueId : uniqueIds) {
			Fetch f = new Fetch(uniqueId, indexName);
			f.setResultFetchType(FetchType.FULL);
			f.setAssociatedFetchType(FetchType.NONE);
			if (documentFields != null) {
				f.setDocumentFields(documentFields);
			}

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
			f.setTimestamp(scoredResult.getTimestamp());
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
	public BatchFetchResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		BatchFetchResponse batchFetchResponse = service.batchFetch(getRequest());

		return new BatchFetchResult(batchFetchResponse);
	}

}
