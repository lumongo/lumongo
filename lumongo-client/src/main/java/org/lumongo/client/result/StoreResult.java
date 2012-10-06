package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.StoreResponse;

public class StoreResult extends Result {

	@SuppressWarnings("unused")
	private StoreResponse storeResponse;

	public StoreResult(StoreResponse storeResponse) {
		this.storeResponse = storeResponse;
	}

}
