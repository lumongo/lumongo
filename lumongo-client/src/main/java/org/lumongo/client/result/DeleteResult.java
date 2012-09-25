package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.DeleteResponse;

public class DeleteResult extends Result {

	public DeleteResult(DeleteResponse deleteResponse, long durationInMs) {
		super(durationInMs);
	}

}
