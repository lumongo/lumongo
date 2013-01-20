package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.OptimizeResponse;

public class OptimizeIndexResult extends Result {

	@SuppressWarnings("unused")
	private OptimizeResponse optimizeResponse;

	public OptimizeIndexResult(OptimizeResponse optimizeResponse) {
		this.optimizeResponse = optimizeResponse;
	}

}
