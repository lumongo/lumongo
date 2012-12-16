package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.ClearResponse;

public class ClearIndexResult extends Result {

	@SuppressWarnings("unused")
	private ClearResponse clearResponse;

	public ClearIndexResult(ClearResponse clearResponse) {
		this.clearResponse = clearResponse;

	}

}
