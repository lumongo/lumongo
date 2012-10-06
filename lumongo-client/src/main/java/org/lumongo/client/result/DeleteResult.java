package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.DeleteResponse;

public class DeleteResult extends Result {

	@SuppressWarnings("unused")
	private DeleteResponse deleteResponse;

	public DeleteResult(DeleteResponse deleteResponse) {
		this.deleteResponse = deleteResponse;
	}

}
