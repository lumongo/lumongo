package org.lumongo.client.result;

import java.util.ArrayList;
import java.util.List;

import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.GroupFetchResponse;

public class BatchFetchResult extends Result {

	@SuppressWarnings("unused")
	private GroupFetchResponse groupFetchResponse;

	private List<FetchResult> fetchResults;

	public BatchFetchResult(GroupFetchResponse groupFetchResponse) {
		this.groupFetchResponse = groupFetchResponse;

		this.fetchResults = new ArrayList<FetchResult>();

		for (FetchResponse ft : groupFetchResponse.getFetchResponseList()) {
			fetchResults.add(new FetchResult(ft));
		}
	}

	public List<FetchResult> getFetchResults() {
		return fetchResults;
	}


}
