package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.BatchDeleteResponse;

public class BatchDeleteResult extends Result {

	@SuppressWarnings("unused")
	private BatchDeleteResponse batchDeleteResponse;

	public BatchDeleteResult(BatchDeleteResponse batchDeleteResponse) {
		this.batchDeleteResponse = batchDeleteResponse;
	}

}
