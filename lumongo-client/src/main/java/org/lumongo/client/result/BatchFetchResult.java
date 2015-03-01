package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.BatchFetchResponse;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.fields.Mapper;

import java.util.ArrayList;
import java.util.List;

public class BatchFetchResult extends Result {
	
	@SuppressWarnings("unused")
	private BatchFetchResponse batchFetchResponse;
	
	private List<FetchResult> fetchResults;
	
	public BatchFetchResult(List<FetchResult> fetchResults) {
		this.fetchResults = fetchResults;
	}
	
	public BatchFetchResult(BatchFetchResponse batchFetchResponse) {
		this.batchFetchResponse = batchFetchResponse;
		
		this.fetchResults = new ArrayList<FetchResult>();
		
		for (FetchResponse ft : batchFetchResponse.getFetchResponseList()) {
			fetchResults.add(new FetchResult(ft));
		}
	}
	
	public List<FetchResult> getFetchResults() {
		return fetchResults;
	}
	
	public <T> List<T> getDocuments(Mapper<T> mapper) throws Exception {
		ArrayList<T> list = new ArrayList<T>();
		for (FetchResult fr : fetchResults) {
			if (fr.hasResultDocument()) {
				list.add(fr.getDocument(mapper));
			}
		}
		return list;
	}
	
	@Override
	public String toString() {
		return fetchResults.toString();
	}
	
}
