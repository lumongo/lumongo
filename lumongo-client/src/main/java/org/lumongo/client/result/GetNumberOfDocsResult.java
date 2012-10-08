package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;

public class GetNumberOfDocsResult extends Result {

	private GetNumberOfDocsResponse getNumberOfDocsResponse;

	public GetNumberOfDocsResult(GetNumberOfDocsResponse getNumberOfDocsResponse) {
		this.getNumberOfDocsResponse = getNumberOfDocsResponse;
	}

	public long getNumberOfDocs() {
		return getNumberOfDocsResponse.getNumberOfDocs();
	}
}
