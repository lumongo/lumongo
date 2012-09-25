package org.lumongo.client.result;

import java.util.List;

import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.ResultDocument;

public class FetchResult extends Result {

	private FetchResponse fetchResponse;
	public FetchResult(FetchResponse fetchResponse, long durationInMs) {
		super(durationInMs);
		this.fetchResponse = fetchResponse;
	}

	public ResultDocument getResultDocument() {
		return fetchResponse.getResultDocument();
	}

	public List<AssociatedDocument> getAssociatedDocuments() {
		return fetchResponse.getAssociatedDocumentList();
	}


}
