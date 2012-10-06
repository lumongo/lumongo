package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.ClearResponse;

public class ClearResult extends Result {

	@SuppressWarnings("unused")
	private ClearResponse clearResponse;

	public ClearResult(ClearResponse clearResponse) {
		this.clearResponse = clearResponse;

	}

}
