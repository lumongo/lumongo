package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.IndexDeleteResponse;

public class DeleteIndexResult extends Result {

	@SuppressWarnings("unused")
	private IndexDeleteResponse indexDeleteResponse;

	public DeleteIndexResult(IndexDeleteResponse indexDeleteResponse) {
		this.indexDeleteResponse = indexDeleteResponse;
	}

}
